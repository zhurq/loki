package series

import (
	"context"

	"github.com/grafana/dskit/instrument"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/pkg/logproto"
	"github.com/grafana/loki/pkg/storage/chunk"
	"github.com/grafana/loki/pkg/storage/stores/index/stats"
	"github.com/grafana/loki/pkg/storage/stores/seriesstore"
	loki_instrument "github.com/grafana/loki/pkg/util/instrument"
)

type metrics struct {
	indexQueryLatency *prometheus.HistogramVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		indexQueryLatency: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "loki",
			Name:      "index_request_duration_seconds",
			Help:      "Time (in seconds) spent in serving index query requests",
			Buckets:   prometheus.ExponentialBucketsRange(0.005, 100, 12),
		}, []string{"operation", "status_code"}),
	}
}

type MonitoredReaderWriter struct {
	rw      seriesstore.ReaderWriter
	metrics *metrics
}

func NewMonitoredReaderWriter(rw seriesstore.ReaderWriter, reg prometheus.Registerer) *MonitoredReaderWriter {
	return &MonitoredReaderWriter{
		rw:      rw,
		metrics: newMetrics(reg),
	}
}

func (m MonitoredReaderWriter) GetChunkRefs(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]logproto.ChunkRef, error) {
	var chunks []logproto.ChunkRef

	if err := loki_instrument.TimeRequest(ctx, "chunk_refs", instrument.NewHistogramCollector(m.metrics.indexQueryLatency), instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		chunks, err = m.rw.GetChunkRefs(ctx, userID, from, through, matchers...)
		return err
	}); err != nil {
		return nil, err
	}

	return chunks, nil
}

func (m MonitoredReaderWriter) GetSeries(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]labels.Labels, error) {
	var lbls []labels.Labels
	if err := loki_instrument.TimeRequest(ctx, "series", instrument.NewHistogramCollector(m.metrics.indexQueryLatency), instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		lbls, err = m.rw.GetSeries(ctx, userID, from, through, matchers...)
		return err
	}); err != nil {
		return nil, err
	}

	return lbls, nil
}

func (m MonitoredReaderWriter) LabelValuesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string, labelName string, matchers ...*labels.Matcher) ([]string, error) {
	var values []string
	if err := loki_instrument.TimeRequest(ctx, "label_values", instrument.NewHistogramCollector(m.metrics.indexQueryLatency), instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		values, err = m.rw.LabelValuesForMetricName(ctx, userID, from, through, metricName, labelName, matchers...)
		return err
	}); err != nil {
		return nil, err
	}

	return values, nil
}

func (m MonitoredReaderWriter) LabelNamesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string) ([]string, error) {
	var values []string
	if err := loki_instrument.TimeRequest(ctx, "label_names", instrument.NewHistogramCollector(m.metrics.indexQueryLatency), instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		values, err = m.rw.LabelNamesForMetricName(ctx, userID, from, through, metricName)
		return err
	}); err != nil {
		return nil, err
	}

	return values, nil
}

func (m MonitoredReaderWriter) Stats(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) (*stats.Stats, error) {
	var sts *stats.Stats
	if err := loki_instrument.TimeRequest(ctx, "stats", instrument.NewHistogramCollector(m.metrics.indexQueryLatency), instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		sts, err = m.rw.Stats(ctx, userID, from, through, matchers...)
		return err
	}); err != nil {
		return nil, err
	}

	return sts, nil
}

func (m MonitoredReaderWriter) Volume(ctx context.Context, userID string, from, through model.Time, limit int32, targetLabels []string, aggregateBy string, matchers ...*labels.Matcher) (*logproto.VolumeResponse, error) {
	var vol *logproto.VolumeResponse
	if err := loki_instrument.TimeRequest(ctx, "volume", instrument.NewHistogramCollector(m.metrics.indexQueryLatency), instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		vol, err = m.rw.Volume(ctx, userID, from, through, limit, targetLabels, aggregateBy, matchers...)
		return err
	}); err != nil {
		return nil, err
	}

	return vol, nil
}

func (m MonitoredReaderWriter) SetChunkFilterer(chunkFilter seriesstore.RequestChunkFilterer) {
	m.rw.SetChunkFilterer(chunkFilter)
}

func (m MonitoredReaderWriter) IndexChunk(ctx context.Context, from, through model.Time, chk chunk.Chunk) error {
	return loki_instrument.TimeRequest(ctx, "index_chunk", instrument.NewHistogramCollector(m.metrics.indexQueryLatency), instrument.ErrorCode, func(ctx context.Context) error {
		return m.rw.IndexChunk(ctx, from, through, chk)
	})
}
