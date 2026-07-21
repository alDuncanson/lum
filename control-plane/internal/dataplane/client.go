package dataplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	lumv1 "github.com/alDuncanson/lum/control-plane/internal/gen/lum/v1"
	"github.com/alDuncanson/lum/control-plane/internal/requestid"
)

// ContractVersion changes only when the two binaries can no longer safely
// communicate using the shared proto contract.
const ContractVersion = "1"

type ReadinessState string

const (
	StateStarting         ReadinessState = "starting"
	StateDownloadingModel ReadinessState = "downloading-model"
	StateReady            ReadinessState = "ready"
	StateUnavailable      ReadinessState = "unavailable"
)

const contentFrameSize = 256 * 1024

type HealthResult struct {
	State  ReadinessState
	Detail string
}

type ContractMismatchError struct {
	Got string
}

func (e *ContractMismatchError) Error() string {
	return fmt.Sprintf("data plane contract version mismatch: lum expects %q, lumen reports %q; rebuild both binaries together", ContractVersion, e.Got)
}

// Client is a thin, typed wrapper over the generated gRPC stub. It
// exists so the rest of the control plane imports one small package
// (dataplane) instead of generated code, and so connection setup lives
// in exactly one place.
type Client struct {
	conn *grpc.ClientConn
	rpc  lumv1.DataPlaneClient
}

// Dial creates the lazy gRPC connection over the private Unix socket.
func Dial(socketPath string) (*Client, error) {
	absolutePath, err := filepath.Abs(socketPath)
	if err != nil {
		return nil, fmt.Errorf("resolving data plane socket path: %w", err)
	}
	target := (&url.URL{Scheme: "unix", Path: absolutePath}).String()
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(requestIDInterceptor),
	)
	if err != nil {
		return nil, fmt.Errorf("creating data plane client: %w", err)
	}
	return &Client{conn: conn, rpc: lumv1.NewDataPlaneClient(conn)}, nil
}

func requestIDInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	id := requestid.FromContext(ctx)
	if id == "" {
		ctx, id = requestid.New(ctx)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, requestid.Header, id)
	started := time.Now()
	err := invoker(ctx, method, req, reply, cc, opts...)
	logRPC(ctx, id, method, started, err)
	return err
}

func logRPC(ctx context.Context, id, method string, started time.Time, err error) {
	code := codes.OK
	level := slog.LevelInfo
	if err != nil {
		code = status.Code(err)
		level = slog.LevelWarn
		if method == "/lum.v1.DataPlane/Health" && code == codes.Unavailable {
			level = slog.LevelDebug
		}
	}
	slog.Log(ctx, level, "data plane RPC",
		"request_id", id,
		"method", method,
		"code", code.String(),
		"took", time.Since(started).Round(time.Millisecond),
	)
}

func (c *Client) Close() error { return c.conn.Close() }

// WaitReady polls Health until the data plane is ready, the context is
// cancelled, or an incompatible peer is reached. Unreachable, transitional,
// and self-reported unavailable states remain monitorable.
func (c *Client) WaitReady(ctx context.Context) error {
	if requestid.FromContext(ctx) == "" {
		ctx, _ = requestid.New(ctx)
	}
	for {
		callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		health, err := c.Health(callCtx)
		cancel()
		if err == nil {
			if health.State == StateReady {
				return nil
			}
		} else if _, fatal := err.(*ContractMismatchError); fatal {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Health reports lifecycle state and validates the peer contract before any
// caller may treat the data plane as ready.
func (c *Client) Health(ctx context.Context) (HealthResult, error) {
	resp, err := c.rpc.Health(ctx, &lumv1.HealthRequest{})
	if err != nil {
		return HealthResult{State: StateUnavailable, Detail: err.Error()}, err
	}
	if got := resp.GetContractVersion(); got != ContractVersion {
		err := &ContractMismatchError{Got: got}
		return HealthResult{State: StateUnavailable, Detail: err.Error()}, err
	}
	return HealthResult{State: readinessState(resp), Detail: resp.GetDetail()}, nil
}

func readinessState(resp *lumv1.HealthResponse) ReadinessState {
	switch resp.GetState() {
	case lumv1.ReadinessState_READINESS_STATE_STARTING:
		return StateStarting
	case lumv1.ReadinessState_READINESS_STATE_DOWNLOADING_MODEL:
		return StateDownloadingModel
	case lumv1.ReadinessState_READINESS_STATE_READY:
		return StateReady
	case lumv1.ReadinessState_READINESS_STATE_UNAVAILABLE:
		return StateUnavailable
	default:
		// Compatibility with data planes built before the additive state field.
		if resp.GetReady() {
			return StateReady
		}
		return StateStarting
	}
}

// IngestDocument runs the full pipeline for one document and returns
// the number of chunks stored.
func (c *Client) IngestDocument(
	ctx context.Context,
	documentID, sourceID, uri, mimeType string,
	content []byte,
	previousChunkCount uint32,
) (uint32, error) {
	resp, err := c.rpc.IngestDocument(ctx, &lumv1.IngestDocumentRequest{
		DocumentId:         documentID,
		SourceId:           sourceID,
		Uri:                uri,
		MimeType:           mimeType,
		Content:            content,
		PreviousChunkCount: previousChunkCount,
	})
	if err != nil {
		return 0, fmt.Errorf("ingest %s: %w", uri, err)
	}
	return resp.GetChunkCount(), nil
}

// IngestBatchDocument is one document sent through the streaming batch RPC.
type IngestBatchDocument struct {
	DocumentID         string
	SourceID           string
	URI                string
	MimeType           string
	Content            []byte
	PreviousChunkCount uint32
}

// IngestBatchResult is the ordered outcome for one batch document.
type IngestBatchResult struct {
	DocumentID string
	ChunkCount uint32
	Err        error
}

// IngestBatch streams bounded content frames and returns one ordered result
// per document. Older data planes fall back to the unary RPC when possible.
func (c *Client) IngestBatch(
	ctx context.Context,
	documents []IngestBatchDocument,
) ([]IngestBatchResult, error) {
	id := requestid.FromContext(ctx)
	if id == "" {
		ctx, id = requestid.New(ctx)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, requestid.Header, id)
	started := time.Now()
	stream, err := c.rpc.IngestBatch(ctx)
	if err == nil {
	sendFrames:
		for _, document := range documents {
			err = stream.Send(&lumv1.IngestBatchRequest{Frame: &lumv1.IngestBatchRequest_Document{
				Document: &lumv1.IngestBatchDocumentHeader{
					DocumentId:         document.DocumentID,
					SourceId:           document.SourceID,
					Uri:                document.URI,
					MimeType:           document.MimeType,
					PreviousChunkCount: document.PreviousChunkCount,
					ContentLength:      uint64(len(document.Content)),
				},
			}})
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = nil // CloseAndRecv carries the authoritative RPC status.
				}
				break sendFrames
			}
			for offset := 0; offset < len(document.Content); offset += contentFrameSize {
				end := min(offset+contentFrameSize, len(document.Content))
				err = stream.Send(&lumv1.IngestBatchRequest{Frame: &lumv1.IngestBatchRequest_Content{
					Content: document.Content[offset:end],
				}})
				if err != nil {
					if errors.Is(err, io.EOF) {
						err = nil
					}
					break sendFrames
				}
			}
			err = stream.Send(&lumv1.IngestBatchRequest{Frame: &lumv1.IngestBatchRequest_EndDocument{
				EndDocument: &lumv1.IngestBatchEndDocument{},
			}})
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = nil
				}
				break sendFrames
			}
		}
	}
	var response *lumv1.IngestBatchResponse
	if err == nil {
		response, err = stream.CloseAndRecv()
	}
	logRPC(ctx, id, lumv1.DataPlane_IngestBatch_FullMethodName, started, err)
	if status.Code(err) == codes.Unimplemented {
		return c.ingestBatchUnaryFallback(ctx, documents)
	}
	if err != nil {
		return nil, fmt.Errorf("ingest batch: %w", err)
	}
	if len(response.GetDocuments()) != len(documents) {
		return nil, fmt.Errorf("ingest batch returned %d results for %d documents", len(response.GetDocuments()), len(documents))
	}

	results := make([]IngestBatchResult, len(documents))
	for index, result := range response.GetDocuments() {
		expectedID := documents[index].DocumentID
		if result.GetDocumentId() != expectedID {
			return nil, fmt.Errorf("ingest batch result %d has document ID %q, expected %q", index, result.GetDocumentId(), expectedID)
		}
		results[index].DocumentID = expectedID
		switch {
		case result.GetSuccess() != nil:
			results[index].ChunkCount = result.GetSuccess().GetChunkCount()
		case result.GetFailure() != nil:
			results[index].Err = fmt.Errorf("%s: %s", result.GetFailure().GetStage(), result.GetFailure().GetMessage())
		default:
			return nil, fmt.Errorf("ingest batch result %d has no outcome", index)
		}
	}
	return results, nil
}

func (c *Client) ingestBatchUnaryFallback(
	ctx context.Context,
	documents []IngestBatchDocument,
) ([]IngestBatchResult, error) {
	results := make([]IngestBatchResult, len(documents))
	for index, document := range documents {
		results[index].DocumentID = document.DocumentID
		results[index].ChunkCount, results[index].Err = c.IngestDocument(
			ctx,
			document.DocumentID,
			document.SourceID,
			document.URI,
			document.MimeType,
			document.Content,
			document.PreviousChunkCount,
		)
	}
	return results, nil
}

// DeleteDocument removes a document's chunks from the vector index.
func (c *Client) DeleteDocument(ctx context.Context, documentID string, chunkCount uint32) error {
	_, err := c.rpc.DeleteDocument(ctx, &lumv1.DeleteDocumentRequest{
		DocumentId: documentID,
		ChunkCount: chunkCount,
	})
	return err
}

// SearchResult mirrors the proto SearchResult as a plain Go struct so
// API handlers don't marshal generated types directly.
type SearchResult struct {
	DocumentID string  `json:"document_id"`
	SourceID   string  `json:"source_id"`
	URI        string  `json:"uri"`
	ChunkIndex uint32  `json:"chunk_index"`
	Score      float32 `json:"score"`
	Text       string  `json:"text"`
}

// Search embeds the query in the data plane and returns nearest chunks.
func (c *Client) Search(ctx context.Context, query string, limit uint32) ([]SearchResult, error) {
	resp, err := c.rpc.Search(ctx, &lumv1.SearchRequest{Query: query, Limit: limit})
	if err != nil {
		return nil, err
	}
	results := make([]SearchResult, 0, len(resp.GetResults()))
	for _, r := range resp.GetResults() {
		results = append(results, SearchResult{
			DocumentID: r.GetDocumentId(),
			SourceID:   r.GetSourceId(),
			URI:        r.GetUri(),
			ChunkIndex: r.GetChunkIndex(),
			Score:      r.GetScore(),
			Text:       r.GetText(),
		})
	}
	return results, nil
}
