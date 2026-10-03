// Package tenancy carries request-scoped values, such as the tenant and the acting user, in contexts.
package tenancy

import "context"

type key string

// With returns a context that carries a value under a name.
func With(ctx context.Context, name, value string) context.Context {
	return context.WithValue(ctx, key(name), value)
}

// Value returns the value a context carries under a name, or "".
func Value(ctx context.Context, name string) string {
	value, _ := ctx.Value(key(name)).(string)
	return value
}
