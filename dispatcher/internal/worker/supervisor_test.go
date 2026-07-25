package worker

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	lumv1 "github.com/alDuncanson/lum/dispatcher/internal/gen/lum/v1"
)

type mismatchedWorker struct {
	lumv1.UnimplementedWorkerServer
}

type healthWorker struct {
	lumv1.UnimplementedWorkerServer
	response *lumv1.HealthResponse
	mu       sync.Mutex
	calls    int
	readyOn  int
}

func (s *healthWorker) Health(context.Context, *lumv1.HealthRequest) (*lumv1.HealthResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.readyOn > 0 && s.calls >= s.readyOn {
		return &lumv1.HealthResponse{Ready: true, ContractVersion: ContractVersion, State: lumv1.ReadinessState_READINESS_STATE_READY}, nil
	}
	return s.response, nil
}

func (mismatchedWorker) Health(context.Context, *lumv1.HealthRequest) (*lumv1.HealthResponse, error) {
	return &lumv1.HealthResponse{Ready: true, ContractVersion: "old"}, nil
}

type batchWorker struct {
	lumv1.UnimplementedWorkerServer
	mu       sync.Mutex
	contents [][]byte
	maxFrame int
}

type legacyWorker struct {
	lumv1.UnimplementedWorkerServer
}

func listenUnix(t *testing.T) net.Listener {
	t.Helper()
	file, err := os.CreateTemp("/tmp", "lum-*.sock")
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	_ = file.Close()
	_ = os.Remove(path)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	return listener
}

func (legacyWorker) IngestDocument(context.Context, *lumv1.IngestDocumentRequest) (*lumv1.IngestDocumentResponse, error) {
	return &lumv1.IngestDocumentResponse{ChunkCount: 7}, nil
}

func (s *batchWorker) IngestBatch(stream lumv1.Worker_IngestBatchServer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var documentID string
	var content []byte
	var results []*lumv1.IngestBatchDocumentResult
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch value := frame.GetFrame().(type) {
		case *lumv1.IngestBatchRequest_Document:
			documentID = value.Document.GetDocumentId()
			content = nil
		case *lumv1.IngestBatchRequest_Content:
			if len(value.Content) > s.maxFrame {
				s.maxFrame = len(value.Content)
			}
			content = append(content, value.Content...)
		case *lumv1.IngestBatchRequest_EndDocument:
			s.contents = append(s.contents, bytes.Clone(content))
			results = append(results, &lumv1.IngestBatchDocumentResult{
				DocumentId: documentID,
				Outcome: &lumv1.IngestBatchDocumentResult_Success{
					Success: &lumv1.IngestBatchDocumentSuccess{ChunkCount: 1},
				},
			})
		}
	}
	return stream.SendAndClose(&lumv1.IngestBatchResponse{Documents: results})
}

func TestSupervisorReapsUnexpectedExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a shell script")
	}

	bin := filepath.Join(t.TempDir(), "lum-worker")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 7\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	sup, err := Spawn(bin, t.TempDir(), "127.0.0.1:0", "quantized")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(sup.cmd.Args, " "); !strings.Contains(got, "--embedding-model quantized") {
		t.Fatalf("child args %q do not include selected embedding model", got)
	}
	if got := strings.Join(sup.cmd.Args, " "); !strings.Contains(got, "--grpc-socket 127.0.0.1:0") {
		t.Fatalf("child args %q do not include selected gRPC socket", got)
	}
	select {
	case <-sup.done:
	case <-time.After(5 * time.Second):
		t.Fatal("child exit was not reaped")
	}
	if sup.cmd.ProcessState == nil || !sup.cmd.ProcessState.Exited() {
		t.Fatal("child has no exited process state after supervisor completed")
	}

	// Stop remains safe after an unexpected exit and must not call Wait twice.
	sup.Stop()
}

func TestSupervisorStopClosesParentLivenessPipe(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a shell script")
	}

	dataDir := t.TempDir()
	bin := filepath.Join(t.TempDir(), "lum-worker")
	script := "#!/bin/sh\ntrap '' INT\ntouch \"$4/ready\"\ncat >/dev/null\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	sup, err := Spawn(bin, dataDir, filepath.Join(dataDir, "lum-worker.sock"), "standard")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sup.Stop)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(dataDir, "ready")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}

	started := time.Now()
	sup.Stop()
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Stop took %v; stdin EOF did not stop child promptly", elapsed)
	}
}

func TestWaitReadyRejectsContractMismatch(t *testing.T) {
	listener := listenUnix(t)
	server := grpc.NewServer()
	lumv1.RegisterWorkerServer(server, mismatchedWorker{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	client, err := Dial(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	health, healthErr := client.Health(context.Background())
	if healthErr == nil || health.State == StateReady {
		t.Fatalf("Health() = (%+v, %v), mismatched ready peer must be unavailable", health, healthErr)
	}
	err = client.WaitReady(context.Background())
	if err == nil || !strings.Contains(err.Error(), "contract version mismatch") {
		t.Fatalf("WaitReady error = %v, want contract version mismatch", err)
	}
}

func TestDialEscapesSocketPath(t *testing.T) {
	file, err := os.CreateTemp("/tmp", "lum-*.sock")
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name() + "#?.sock"
	_ = file.Close()
	_ = os.Remove(file.Name())
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
		_ = os.Remove(path)
	})
	server := grpc.NewServer()
	lumv1.RegisterWorkerServer(server, &healthWorker{response: &lumv1.HealthResponse{
		Ready: true, ContractVersion: ContractVersion, State: lumv1.ReadinessState_READINESS_STATE_READY,
	}})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(workingDir, path)
	if err != nil {
		t.Fatal(err)
	}
	client, err := Dial(relative)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.Health(ctx); err != nil {
		t.Fatalf("dialing escaped relative socket path: %v", err)
	}
}

func TestReadinessStateAndLegacyFallback(t *testing.T) {
	tests := []struct {
		name     string
		response *lumv1.HealthResponse
		want     ReadinessState
	}{
		{"starting", &lumv1.HealthResponse{State: lumv1.ReadinessState_READINESS_STATE_STARTING}, StateStarting},
		{"downloading", &lumv1.HealthResponse{State: lumv1.ReadinessState_READINESS_STATE_DOWNLOADING_MODEL}, StateDownloadingModel},
		{"ready", &lumv1.HealthResponse{State: lumv1.ReadinessState_READINESS_STATE_READY}, StateReady},
		{"unavailable", &lumv1.HealthResponse{State: lumv1.ReadinessState_READINESS_STATE_UNAVAILABLE}, StateUnavailable},
		{"legacy ready", &lumv1.HealthResponse{Ready: true}, StateReady},
		{"legacy loading", &lumv1.HealthResponse{Ready: false}, StateStarting},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := readinessState(test.response); got != test.want {
				t.Fatalf("readinessState() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestWaitReadyMonitorsUnavailableUntilReady(t *testing.T) {
	listener := listenUnix(t)
	server := grpc.NewServer()
	fake := &healthWorker{response: &lumv1.HealthResponse{
		ContractVersion: ContractVersion,
		State:           lumv1.ReadinessState_READINESS_STATE_UNAVAILABLE,
		Detail:          "model initialization failed",
	}, readyOn: 2}
	lumv1.RegisterWorkerServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	client, err := Dial(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = client.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady returned on nonfatal unavailable state: %v", err)
	}
}

func TestIngestBatchStreamsBoundedFramesAcrossDocuments(t *testing.T) {
	service := &batchWorker{}
	listener := listenUnix(t)
	server := grpc.NewServer()
	lumv1.RegisterWorkerServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	client, err := Dial(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	large := bytes.Repeat([]byte("x"), 5*1024*1024)
	small := []byte("small document")
	results, err := client.IngestBatch(context.Background(), []IngestBatchDocument{
		{DocumentID: "large", SourceID: "source", URI: "/large", MimeType: "text/plain", Content: large},
		{DocumentID: "small", SourceID: "source", URI: "/small", MimeType: "text/plain", Content: small},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].Err != nil || results[1].Err != nil {
		t.Fatalf("unexpected results: %#v", results)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.maxFrame > contentFrameSize {
		t.Fatalf("largest content frame = %d, want <= %d", service.maxFrame, contentFrameSize)
	}
	if len(service.contents) != 2 || !bytes.Equal(service.contents[0], large) || !bytes.Equal(service.contents[1], small) {
		t.Fatal("streamed document content did not round trip")
	}
}

func TestIngestBatchFallsBackToLegacyUnaryRPC(t *testing.T) {
	listener := listenUnix(t)
	server := grpc.NewServer()
	lumv1.RegisterWorkerServer(server, legacyWorker{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	client, err := Dial(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	results, err := client.IngestBatch(context.Background(), []IngestBatchDocument{{
		DocumentID: "doc", SourceID: "source", URI: "/doc", MimeType: "text/plain", Content: []byte("content"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Err != nil || results[0].ChunkCount != 7 {
		t.Fatalf("unexpected fallback results: %#v", results)
	}
}

type captureSearchWorker struct {
	lumv1.UnimplementedWorkerServer
	mu       sync.Mutex
	requests []*lumv1.SearchRequest
}

func (s *captureSearchWorker) Search(_ context.Context, req *lumv1.SearchRequest) (*lumv1.SearchResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, req)
	return &lumv1.SearchResponse{}, nil
}

func TestSearchSendsSourceIDFilterToWorker(t *testing.T) {
	service := &captureSearchWorker{}
	listener := listenUnix(t)
	server := grpc.NewServer()
	lumv1.RegisterWorkerServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	client, err := Dial(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if _, err := client.Search(context.Background(), "wild yeast", 5, "source-123"); err != nil {
		t.Fatal(err)
	}

	service.mu.Lock()
	defer service.mu.Unlock()
	if len(service.requests) != 1 {
		t.Fatalf("requests received = %d, want 1", len(service.requests))
	}
	got := service.requests[0]
	if got.SourceId != "source-123" || got.Query != "wild yeast" || got.Limit != 5 {
		t.Fatalf("request = %+v, want query=\"wild yeast\" limit=5 source_id=source-123", got)
	}
}
