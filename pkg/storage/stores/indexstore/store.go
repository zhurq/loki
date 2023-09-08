package indexstore

import (
	"context"

	"github.com/grafana/loki/pkg/storage/chunk"
	"github.com/prometheus/common/model"
)

type Writer interface {
	IndexChunk(ctx context.Context, from, through model.Time, chk chunk.Chunk) error
}

type Store interface {
	Writer
}
