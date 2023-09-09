package seriesstore

import (
	"context"

	"github.com/grafana/loki/pkg/logproto"
	"github.com/grafana/loki/pkg/storage/stores/index/stats"
	"github.com/grafana/loki/pkg/storage/stores/indexstore"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

// RequestChunkFilterer creates ChunkFilterer for a given request context.
type RequestChunkFilterer interface {
	ForRequest(ctx context.Context) Filterer
}

// Filterer filters chunks based on the metric.
type Filterer interface {
	ShouldFilter(metric labels.Labels) bool
}

type Filterable interface {
	SetChunkFilterer(chunkFilter RequestChunkFilterer)
}

type BaseReader interface {
	GetSeries(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]labels.Labels, error)
	LabelValuesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string, labelName string, matchers ...*labels.Matcher) ([]string, error)
	LabelNamesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string) ([]string, error)
}

type StatsReader interface {
	Stats(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) (*stats.Stats, error)
	Volume(ctx context.Context, userID string, from, through model.Time, limit int32, targetLabels []string, aggregateBy string, matchers ...*labels.Matcher) (*logproto.VolumeResponse, error)
}

type ChunkRefsReader interface {
	GetChunkRefs(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]logproto.ChunkRef, error)
}

type Reader interface {
	BaseReader
	StatsReader
}

type SeriesReader interface {
	BaseReader
	StatsReader
	ChunkRefsReader
	Filterable
}

type ReaderWriter interface {
	SeriesReader
	indexstore.Writer
}
