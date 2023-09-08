package chunkstore

import (
	"context"

	"github.com/grafana/loki/pkg/storage/chunk"
	"github.com/grafana/loki/pkg/storage/chunk/fetcher"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

type ChunkWriter interface {
	Put(ctx context.Context, chunks []chunk.Chunk) error
	PutOne(ctx context.Context, from, through model.Time, chunk chunk.Chunk) error
}

type ChunkFetcherProvider interface {
	GetChunkFetcher(tm model.Time) *fetcher.Fetcher
}

type ChunkFetcher interface {
	GetChunks(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([][]chunk.Chunk, []*fetcher.Fetcher, error)
}

type Store interface {
	ChunkWriter
	ChunkFetcher
	ChunkFetcherProvider
}
