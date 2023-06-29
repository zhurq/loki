# PoC: Replacing mmap

This proof-of-concept shows how `mmap` could be replaced by a file-backed buffered file reader.
It only replaces the index reader of TSDB, however, `mmap` is still used for writing the TSDB index.

## Dependencies

Following files are copied from the [grafana/mimir](https://github.com/grafana/mimir) project at commit `65a50b01a4106520db9ef6fd3aad68adbc1aa4a8`:

### `pkg/storage/stores/tsdb/index/binaryreader/reader.go`

Copied from https://github.com/grafana/mimir/blob/65a50b01a4106520db9ef6fd3aad68adbc1aa4a8/pkg/storegateway/indexheader/encoding/reader.go

This file contains the pooled buffered file reader implementation.

_No modifications to this file._

### `pkg/storage/stores/tsdb/index/binaryreader/factory.go`

Copied from https://github.com/grafana/mimir/blob/65a50b01a4106520db9ef6fd3aad68adbc1aa4a8/pkg/storegateway/indexheader/encoding/factory.go

This file contains functions to obtain decoding buffers at a specific offset. The decoders are backed by a pooled, buffered fie reader. The factory can also verify the checksum of a buffer when a new one is instantiated.

A new function `NewDecbufUvarintAtChecked(offset int, table *crc32.Table) Decbuf` had to be added to decode an unsigned variable length integer as buffer length e.g. used in the encoding of the Series sequence (https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/index.md#series). This function is pretty much the same as `NewDecbufAtChecked(offset int, table *crc32.Table) Decbuf` with the difference that it decodes a uvarint rather than an int.

### `pkg/storage/stores/tsdb/index/binaryreader/encoding.go`

Copied from https://github.com/grafana/mimir/blob/65a50b01a4106520db9ef6fd3aad68adbc1aa4a8/pkg/storegateway/indexheader/encoding/encoding.go

This file contains the decoder for the binary data. It contains functionality to read individual types from the binary format.

The `CheckCrc32()` function has been modified to also return the actual and expected checksums. This was mainly needed to keep the error message consistent across the mmap based implementation and the buffered reader implementation.

Additionally, three methods `Get() []byte`, `Uvarint32() uint32`, and `Be64int64() int64` where added, because these are also used in the decoder implementation of the mmap based reader.

### `pkg/storage/stores/tsdb/index/binaryreader/symbols.go`

Copied from https://github.com/grafana/mimir/blob/65a50b01a4106520db9ef6fd3aad68adbc1aa4a8/pkg/storegateway/indexheader/index/symbols.go

Only updated the symbols constructor to accommodate for the index version >= v2. Further more, the new symbols implementation also required the implementation of `Size()` and `Iter()`. The latter returns the `symbolsIter` implementation of `StringIter`.

### `StreamBinaryReader` implementation

I started with copying the `Reader` implementation from the `index.go` file of the package `pkg/storage/stores/tsdb/index` into a new `stream_binary_reader.go` file.
Then gradually replaced usages of the decoding buffer `DecBuf` from the `github.com/prometheus/prometheus/tsdb/encoding` package with the decoder obtained from the decoder buffer factory `DecBufferFactory` (which is backed by the buffered file reader).

This was mostly straight forward, except that in some places offset calculations had to be adjusted due to the fact that there are some small differences how Prometheus' `DecBuf` and Mimir's `Decbuf` work. E.g. Prometheus' decoding buffer starts at the provider offset, whereas Mimir's buffer starts at offset+4 (after length bytes) and also does contain the 4 bytes of the checksum at the end.

One tricky part was to close the decoding buffer after usage so the buffer and file pointer resources are released properly.

Both the `Reader` and the `StreamBinaryReader` have an internal `Decoder`, which provides additional functionality to Promtheus' `DecBuf`. However this decoder is using Promtheus' decoder under the hood, and therefore operates on a byte slice. This functionality hasn't changed in the new reader implementation. Therefore, whenever it uses the internal decoder, we call `Get()` on Mimir's `Decbuf` to obtain all bytes from the buffer. This should be changed so the decoder also operates directly on Mimir's decoding buffer.

### Testing

For testing, I copied the relevant reader tests from `index_test.go` and replaced the constructor of the index reader. Once these were passing, I replaced the reader implementation in the constructor for the TSBD index (`NewTSDBIndexFromFile()`) so the new reader is used throughout tests, speficically `single_file_index_test.go`.

### Benchmarks

No benchmarks so far.
