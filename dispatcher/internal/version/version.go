// Package version owns the user-facing Lum product version.
package version

// Value is replaced by release builds through -ldflags. Source builds report
// "dev" so they cannot be mistaken for a packaged release.
var Value = "dev"
