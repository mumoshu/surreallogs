package metadata

import "context"

type Provider interface {
	// Fetch fetches the metadata associated to the log file at the given path,
	// and fills the given metadata map with the fetched metadata.
	//
	// The reason this function doesn't allocate a map and return it is that
	// the caller can reuse the same map for multiple calls to this function,
	// and the caller can also pre-allocate the map with a reasonable size.
	Fetch(ctx context.Context, path string, metadata map[string]interface{}) error
}

func NewNoop() *NoopProvider {
	return &NoopProvider{}
}

type NoopProvider struct{}

func (p *NoopProvider) Fetch(ctx context.Context, path string, metadata map[string]interface{}) error {
	return nil
}
