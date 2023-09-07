package storage

import (
	"context"

	"github.com/grafana/loki/pkg/logproto"
	"github.com/grafana/loki/pkg/logql"
	"github.com/grafana/loki/pkg/storage/config"
	"github.com/grafana/loki/pkg/storage/stores"
	"github.com/grafana/loki/pkg/storage/stores/index"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

// TODO(chaudum): Come up with better name.
type QuerierStore interface {
	logql.Querier
	SelectSeries(ctx context.Context, req logql.SelectLogParams) ([]logproto.SeriesIdentifier, error)
}

// TODO(chaudum): Come up with better name.
type ReadStore interface {
	index.BaseReader
	index.MetadataReader
	stores.ChunkFetcher
	stores.ChunkFetcherProvider
	Stop()
}

// TODO(chaudum): Come up with better name.
type ReadWriteStore interface {
	ReadStore
	stores.ChunkWriter
	index.Filterable
}

// TODO(chaudum) Store should inherit ReadStore?
type Store interface {
	ReadWriteStore
	QuerierStore
	SchemaConfigProvider
}

type SchemaConfigProvider interface {
	GetSchemaConfigs() []config.PeriodConfig
}

type IngesterQuerier interface {
	index.MetadataReader
	GetChunkIDs(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]string, error)
}
