package stores

import (
	"github.com/grafana/loki/pkg/storage/stores/chunkstore"
	"github.com/grafana/loki/pkg/storage/stores/seriesstore"
)

type Store interface {
	seriesstore.BaseReader
	seriesstore.StatsReader
	seriesstore.Filterable
	chunkstore.ChunkWriter
	chunkstore.ChunkFetcher
	chunkstore.ChunkFetcherProvider
	Stop()
}
