package respbuilder

import "context"

// Recipients avoid allocating when assigning to an interface{}, context keys often have concrete type struct{}.
type respCtxKey struct{}

var respTracerKey = respCtxKey{}

type Tracer struct {
	RemoteAddr string
	AppTraceID string
}

// Inject inject Tracer object into context.
// As Go doc said: https://golang.org/pkg/context/#WithValue
// Use context Values only for request-scoped data that transits processes and APIs,
// not for passing optional parameters to functions.
// https://blog.golang.org/context
func Inject(ctx context.Context, stuff Tracer) context.Context {
	return context.WithValue(ctx, respTracerKey, stuff)
}

// Extract get Tracer information from context
func Extract(ctx context.Context) (Tracer, bool) {
	stuff, ok := ctx.Value(respTracerKey).(Tracer)
	if !ok {
		return Tracer{}, false
	}

	return stuff, ok
}

// MustExtract will extract Tracer without false condition.
// When Tracer is not exist, it will return empty Tracer instead of error.
func MustExtract(ctx context.Context) Tracer {
	stuff, _ := Extract(ctx)
	return stuff
}
