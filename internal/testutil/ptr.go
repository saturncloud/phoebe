// Package testutil holds small helpers shared by test packages.
package testutil

// Ptr returns a pointer to a copy of v. Tests use it to build optional
// fields such as *int64 limits inline.
func Ptr[T any](v T) *T { return &v }
