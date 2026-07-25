// Package requestid carries correlation IDs across HTTP handlers,
// background scans, and worker gRPC calls.
package requestid

import (
	"context"

	"github.com/google/uuid"
)

// Header is used for both HTTP headers and gRPC metadata.
const Header = "x-request-id"

type contextKey struct{}

// New returns a child context with a newly generated request ID.
func New(ctx context.Context) (context.Context, string) {
	id := uuid.NewString()
	return WithValue(ctx, id), id
}

// WithValue returns a child context carrying id.
func WithValue(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

// FromContext returns the request ID, if one has been assigned.
func FromContext(ctx context.Context) string {
	id, _ := ctx.Value(contextKey{}).(string)
	return id
}
