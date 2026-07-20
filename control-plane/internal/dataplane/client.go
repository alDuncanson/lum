package dataplane

import (
	"context"
	"fmt"
	"log/slog"
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

// Client is a thin, typed wrapper over the generated gRPC stub. It
// exists so the rest of the control plane imports one small package
// (dataplane) instead of generated code, and so connection setup lives
// in exactly one place.
type Client struct {
	conn *grpc.ClientConn
	rpc  lumv1.DataPlaneClient
}

// Dial creates the (lazy) gRPC connection. Plaintext is fine here and
// only here: the connection never leaves loopback.
func Dial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr,
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
	return err
}

func (c *Client) Close() error { return c.conn.Close() }

// WaitReady polls Health until the data plane answers or the deadline
// passes. The generous default matters: on first run lumen downloads
// the embedding model (~70 MB) before it starts listening.
func (c *Client) WaitReady(ctx context.Context, timeout time.Duration) error {
	if requestid.FromContext(ctx) == "" {
		ctx, _ = requestid.New(ctx)
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		resp, err := c.rpc.Health(callCtx, &lumv1.HealthRequest{})
		cancel()
		if err == nil {
			if got := resp.GetContractVersion(); got != ContractVersion {
				return fmt.Errorf("data plane contract version mismatch: lum expects %q, lumen reports %q; rebuild both binaries together", ContractVersion, got)
			}
			if resp.GetReady() {
				return nil
			}
			lastErr = fmt.Errorf("data plane reports not ready: %s", resp.GetDetail())
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("data plane not ready after %s: %w", timeout, lastErr)
}

// Health reports data plane readiness and detail (model name).
func (c *Client) Health(ctx context.Context) (bool, string) {
	resp, err := c.rpc.Health(ctx, &lumv1.HealthRequest{})
	if err != nil {
		return false, err.Error()
	}
	return resp.GetReady(), resp.GetDetail()
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
