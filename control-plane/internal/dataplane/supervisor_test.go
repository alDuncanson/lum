package dataplane

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

	lumv1 "github.com/alDuncanson/lum/control-plane/internal/gen/lum/v1"
)

type mismatchedDataPlane struct {
	lumv1.UnimplementedDataPlaneServer
}

func (mismatchedDataPlane) Health(context.Context, *lumv1.HealthRequest) (*lumv1.HealthResponse, error) {
	return &lumv1.HealthResponse{Ready: true, ContractVersion: "old"}, nil
}

type batchDataPlane struct {
	lumv1.UnimplementedDataPlaneServer
	mu       sync.Mutex
	contents [][]byte
	maxFrame int
}

type legacyDataPlane struct {
	lumv1.UnimplementedDataPlaneServer
}

func (legacyDataPlane) IngestDocument(context.Context, *lumv1.IngestDocumentRequest) (*lumv1.IngestDocumentResponse, error) {
	return &lumv1.IngestDocumentResponse{ChunkCount: 7}, nil
}

func (s *batchDataPlane) IngestBatch(stream lumv1.DataPlane_IngestBatchServer) error {
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

	bin := filepath.Join(t.TempDir(), "lumen")
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

func TestWaitReadyRejectsContractMismatch(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	lumv1.RegisterDataPlaneServer(server, mismatchedDataPlane{})
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

	err = client.WaitReady(context.Background(), time.Second)
	if err == nil || !strings.Contains(err.Error(), "contract version mismatch") {
		t.Fatalf("WaitReady error = %v, want contract version mismatch", err)
	}
}

func TestIngestBatchStreamsBoundedFramesAcrossDocuments(t *testing.T) {
	service := &batchDataPlane{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	lumv1.RegisterDataPlaneServer(server, service)
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
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	lumv1.RegisterDataPlaneServer(server, legacyDataPlane{})
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
