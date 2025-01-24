package metadata

import (
	"context"
	"sync"
	"time"
)

// CacheProvider is a metadata provider that wraps another metadata provider
// and caches the result of the wrapped provider for specified TTL.
type CacheProvider struct {
	provider Provider

	TTL time.Duration

	*sync.Map
}

type cacheEntry struct {
	timestamp time.Time
	metadata  map[string]interface{}
}

func NewCache(provider Provider, ttl time.Duration) *CacheProvider {
	return &CacheProvider{
		provider: provider,
		TTL:      ttl,
		Map:      &sync.Map{},
	}
}

func (p *CacheProvider) Fetch(ctx context.Context, path string, metadata map[string]interface{}) error {
	// Check if we have a cached entry
	if entry, ok := p.Load(path); ok {
		cached := entry.(cacheEntry)
		if time.Since(cached.timestamp) < p.TTL {
			// Cache hit - copy cached metadata
			for k, v := range cached.metadata {
				metadata[k] = v
			}
			return nil
		}
	}

	// Cache miss or expired - fetch fresh metadata
	cachedMetadata := make(map[string]interface{})
	err := p.provider.Fetch(ctx, path, cachedMetadata)
	if err != nil {
		return err
	}

	// Store in cache
	p.Store(path, cacheEntry{
		metadata:  cachedMetadata,
		timestamp: time.Now(),
	})

	// Copy metadata to output map
	for k, v := range cachedMetadata {
		metadata[k] = v
	}

	return nil
}
