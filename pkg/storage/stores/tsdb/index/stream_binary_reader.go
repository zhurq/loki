package index

import (
	"bytes"
	"fmt"
	"io"
	"sort"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	tsdb_enc "github.com/prometheus/prometheus/tsdb/encoding"

	"github.com/grafana/loki/pkg/storage/stores/tsdb/index/binaryreader"
)

// NewTOCFromDecbuf return parsed TOC from given Decbuf
func NewTOCFromDecbuf(d binaryreader.Decbuf, indexLen int) (*TOC, error) {
	tocOffset := indexLen - indexTOCLen
	if d.ResetAt(tocOffset); d.Err() != nil {
		return nil, d.Err()
	}

	actual, _ := d.CheckCrc32(castagnoliTable)
	if d.Err() != nil {
		return nil, d.Err()
	}

	d.ResetAt(tocOffset)

	return &TOC{
		Symbols:            d.Be64(),
		Series:             d.Be64(),
		LabelIndices:       d.Be64(),
		LabelIndicesTable:  d.Be64(),
		Postings:           d.Be64(),
		PostingsTable:      d.Be64(),
		FingerprintOffsets: d.Be64(),
		Metadata: Metadata{
			From:     d.Be64int64(),
			Through:  d.Be64int64(),
			Checksum: actual,
		},
	}, d.E
}

type StreamBinaryReader struct {
	factory *binaryreader.DecbufFactory
	toc     *TOC

	// Map of LabelName to a list of some LabelValues's position in the offset table.
	// The first and last values for each name are always present.
	postings map[string][]postingOffset
	// For the v1 format, labelname -> labelvalue -> offset.
	postingsV1 map[string]map[string]uint64

	symbols     *binaryreader.Symbols
	nameSymbols map[uint32]string // Cache of the label name symbol lookups,
	// as there are not many and they are half of all lookups.

	fingerprintOffsets FingerprintOffsets

	dec *Decoder

	version int
}

// NewStreamBinaryFileReader returns a new index reader against the given index file.
func NewStreamBinaryReader(path string) (*StreamBinaryReader, error) {
	metrics := binaryreader.NewDecbufFactoryMetrics(nil)
	factory := binaryreader.NewDecbufFactory(path, 128, log.NewNopLogger(), metrics)
	return newStreamBinaryReader(factory)
}

func newStreamBinaryReader(factory *binaryreader.DecbufFactory) (*StreamBinaryReader, error) {
	r := &StreamBinaryReader{
		factory:  factory,
		postings: map[string][]postingOffset{},
	}

	var err error

	// Create a new raw decoding buffer with access to the entire index-header file to
	// read initial version information and the table of contents.
	d := r.factory.NewRawDecbuf()
	defer runutil.CloseWithErrCapture(&err, &d, "new file reader")
	if err = d.Err(); err != nil {
		return nil, fmt.Errorf("cannot create decoding buffer: %w", err)
	}

	// Grab the full length of the index before we read any of it. This is needed
	// so that we can skip directly to the table of contents at the end of file.
	indexSize := d.Len()
	if indexSize < HeaderLen {
		return nil, errors.Wrap(tsdb_enc.ErrInvalidSize, "index header")
	}
	if magic := d.Be32(); magic != MagicIndex {
		return nil, fmt.Errorf("invalid magic number %x", magic)
	}

	r.version = int(d.Byte())
	if r.version != FormatV1 && r.version != FormatV2 && r.version != FormatV3 {
		return nil, errors.Errorf("unknown index file version %d", r.version)
	}

	r.toc, err = NewTOCFromDecbuf(d, indexSize)
	if err != nil {
		return nil, errors.Wrap(err, "read TOC")
	}

	r.symbols, err = binaryreader.NewSymbols(r.factory, r.version, int(r.toc.Symbols))
	if err != nil {
		return nil, errors.Wrap(err, "read symbols")
	}

	if r.version == FormatV1 {
		// Earlier V1 formats don't have a sorted postings offset table, so
		// load the whole offset table into memory.
		r.postingsV1 = map[string]map[string]uint64{}
		if err = BinaryReadOffsetTable(r.factory, r.toc.PostingsTable, func(key []string, off uint64, _ int) error {
			if len(key) != 2 {
				return errors.Errorf("unexpected key length for posting table %d", len(key))
			}
			if _, ok := r.postingsV1[key[0]]; !ok {
				r.postingsV1[key[0]] = map[string]uint64{}
				r.postings[key[0]] = nil // Used to get a list of labelnames in places.
			}
			r.postingsV1[key[0]][key[1]] = off
			return nil
		}); err != nil {
			return nil, errors.Wrap(err, "read postings table")
		}
	} else {
		var lastKey []string
		lastOff := 0
		valueCount := 0
		// For the postings offset table we keep every label name but only every nth
		// label value (plus the first and last one), to save memory.
		if err := BinaryReadOffsetTable(r.factory, r.toc.PostingsTable, func(key []string, _ uint64, off int) error {
			if len(key) != 2 {
				return errors.Errorf("unexpected key length for posting table %d", len(key))
			}
			if _, ok := r.postings[key[0]]; !ok {
				// Next label name.
				r.postings[key[0]] = []postingOffset{}
				if lastKey != nil {
					// Always include last value for each label name.
					r.postings[lastKey[0]] = append(r.postings[lastKey[0]], postingOffset{value: lastKey[1], off: lastOff})
				}
				lastKey = nil
				valueCount = 0
			}
			if valueCount%symbolFactor == 0 {
				r.postings[key[0]] = append(r.postings[key[0]], postingOffset{value: key[1], off: off})
				lastKey = nil
			} else {
				lastKey = key
				lastOff = off
			}
			valueCount++
			return nil
		}); err != nil {
			return nil, errors.Wrap(err, "read postings table")
		}
		if lastKey != nil {
			r.postings[lastKey[0]] = append(r.postings[lastKey[0]], postingOffset{value: lastKey[1], off: lastOff})
		}
		// Trim any extra space in the slices.
		for k, v := range r.postings {
			l := make([]postingOffset, len(v))
			copy(l, v)
			r.postings[k] = l
		}
	}

	r.nameSymbols = make(map[uint32]string, len(r.postings))
	for k := range r.postings {
		if k == "" {
			continue
		}
		off, err := r.symbols.ReverseLookup(k)
		if err != nil {
			return nil, errors.Wrap(err, "reverse symbol lookup")
		}
		r.nameSymbols[off] = k
	}

	r.fingerprintOffsets, err = BinaryReadFingerprintOffsetsTable(r.factory, r.toc.FingerprintOffsets)
	if err != nil {
		return nil, errors.Wrap(err, "loading fingerprint offsets")
	}

	r.dec = newDecoder(r.lookupSymbol, DefaultMaxChunksToBypassMarkerLookup)

	return r, nil
}

// Version returns the file format version of the underlying index.
func (r *StreamBinaryReader) Version() int {
	return r.version
}

// TODO(chaudum): ???
func (r *StreamBinaryReader) RawFileReader() (io.ReadSeeker, error) {
	var err error
	d := r.factory.NewRawDecbuf()
	defer runutil.CloseWithErrCapture(&err, &d, "new file reader")
	if err = d.Err(); err != nil {
		return nil, fmt.Errorf("cannot create decoding buffer: %w", err)
	}
	return bytes.NewReader(d.Get()), nil
}

// PostingsRanges returns a new map of byte range in the underlying index file
// for all postings lists.
func (r *StreamBinaryReader) PostingsRanges() (map[labels.Label]Range, error) {
	m := map[labels.Label]Range{}
	if err := BinaryReadOffsetTable(r.factory, r.toc.PostingsTable, func(key []string, off uint64, _ int) error {
		if len(key) != 2 {
			return errors.Errorf("unexpected key length for posting table %d", len(key))
		}

		// TODO(chaudum): This is kinda expensive, and it's only used for d.Len()
		var err error
		d := r.factory.NewDecbufAtChecked(int(off), castagnoliTable)
		defer runutil.CloseWithErrCapture(&err, &d, "read postings ranges")
		if d.Err() != nil {
			return d.Err()
		}

		m[labels.Label{Name: key[0], Value: key[1]}] = Range{
			Start: int64(off) + 4,
			End:   int64(off) + 4 + int64(d.Len()),
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "read postings table")
	}
	return m, nil
}

// BinaryReadOffsetTable reads an offset table and at the given position calls f for each
// found entry. If f returns an error it stops decoding and returns the received error.
func BinaryReadOffsetTable(factory *binaryreader.DecbufFactory, off uint64, f func([]string, uint64, int) error) error {
	var err error

	d := factory.NewDecbufAtChecked(int(off), castagnoliTable)
	defer runutil.CloseWithErrCapture(&err, &d, "read offset table")

	startLen := d.Len()
	cnt := d.Be32()

	for d.Err() == nil && d.Len() > 0 && cnt > 0 {
		offsetPos := startLen - d.Len()
		keyCount := d.Uvarint()
		// The Postings offset table takes only 2 keys per entry (name and value of label),
		// and the LabelIndices offset table takes only 1 key per entry (a label name).
		// Hence setting the size to max of both, i.e. 2.
		keys := make([]string, 0, 2)

		for i := 0; i < keyCount; i++ {
			keys = append(keys, d.UvarintStr())
		}
		o := d.Uvarint64()
		if d.Err() != nil {
			break
		}
		if err := f(keys, o, offsetPos); err != nil {
			return err
		}
		cnt--
	}
	return d.Err()
}

func BinaryReadFingerprintOffsetsTable(factory *binaryreader.DecbufFactory, off uint64) (FingerprintOffsets, error) {
	var err error

	d := factory.NewDecbufAtChecked(int(off), castagnoliTable)
	defer runutil.CloseWithErrCapture(&err, &d, "read fingerprint offset table")

	cnt := d.Be32()
	res := make(FingerprintOffsets, 0, int(cnt))

	for d.Err() == nil && d.Len() > 0 && cnt > 0 {
		res = append(res, [2]uint64{d.Be64(), d.Be64()})
		cnt--
	}

	return res, d.Err()
}

func (r *StreamBinaryReader) Close() error {
	r.factory.Stop()
	return nil
}

func (r *StreamBinaryReader) lookupSymbol(o uint32) (string, error) {
	if s, ok := r.nameSymbols[o]; ok {
		return s, nil
	}
	return r.symbols.Lookup(o)
}

func (r *StreamBinaryReader) Bounds() (int64, int64) {
	return r.toc.Metadata.From, r.toc.Metadata.Through
}

func (r *StreamBinaryReader) Checksum() uint32 {
	return r.toc.Metadata.Checksum
}

// Symbols returns an iterator over the symbols that exist within the index.
func (r *StreamBinaryReader) Symbols() binaryreader.StringIter {
	return r.symbols.Iter()
}

// SymbolTableSize returns the symbol table size in bytes.
func (r *StreamBinaryReader) SymbolTableSize() uint64 {
	return uint64(r.symbols.Size())
}

// SortedLabelValues returns value tuples that exist for the given label name.
func (r *StreamBinaryReader) SortedLabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	values, err := r.LabelValues(name, matchers...)
	if err == nil && r.version == FormatV1 {
		sort.Strings(values)
	}
	return values, err
}

// LabelValues returns value tuples that exist for the given label name.
// TODO(replay): Support filtering by matchers
func (r *StreamBinaryReader) LabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	if len(matchers) > 0 {
		return nil, errors.Errorf("matchers parameter is not implemented: %+v", matchers)
	}

	if r.version == FormatV1 {
		e, ok := r.postingsV1[name]
		if !ok {
			return nil, nil
		}
		values := make([]string, 0, len(e))
		for k := range e {
			values = append(values, k)
		}
		return values, nil

	}
	e, ok := r.postings[name]
	if !ok {
		return nil, nil
	}
	if len(e) == 0 {
		return nil, nil
	}
	values := make([]string, 0, len(e)*symbolFactor)

	var err error
	d := r.factory.NewDecbufAtChecked(int(r.toc.PostingsTable), castagnoliTable)
	defer runutil.CloseWithErrCapture(&err, &d, "read label values")

	d.Skip(e[0].off)
	lastVal := e[len(e)-1].value

	skip := 0
	for d.Err() == nil {
		if skip == 0 {
			// These are always the same number of bytes,
			// and it's faster to skip than parse.
			skip = d.Len()
			d.Uvarint()            // Key count
			d.UnsafeUvarintBytes() // Label name
			skip -= d.Len()
		} else {
			d.Skip(skip)
		}
		s := string(d.UnsafeUvarintBytes()) // Label value
		values = append(values, s)
		if s == lastVal {
			break
		}
		d.Uvarint64() // Offset.
	}
	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "get postings offset entry")
	}
	return values, nil
}

// LabelNamesFor returns all the label names for the series referred to by IDs.
// The names returned are sorted.
func (r *StreamBinaryReader) LabelNamesFor(ids ...storage.SeriesRef) ([]string, error) {
	var err error
	// Gather offsetsMap the name offsetsMap in the symbol table first
	offsetsMap := make(map[uint32]struct{})
	for _, id := range ids {
		offset := id
		// In version 2+ series IDs are no longer exact references but series are 16-byte padded
		// and the ID is the multiple of 16 of the actual position.
		if r.version >= FormatV2 {
			offset = id * 16
		}

		d := r.factory.NewDecbufAtChecked(int(offset), castagnoliTable)
		defer runutil.CloseWithErrCapture(&err, &d, "label names for")

		offsets, err := LabelNamesOffsets(d)
		if err != nil {
			return nil, errors.Wrap(err, "get label name offsets")
		}
		for _, off := range offsets {
			offsetsMap[off] = struct{}{}
		}
	}

	// Lookup the unique symbols.
	names := make([]string, 0, len(offsetsMap))
	for off := range offsetsMap {
		name, err := r.lookupSymbol(off)
		if err != nil {
			return nil, errors.Wrap(err, "lookup symbol in LabelNamesFor")
		}
		names = append(names, name)
	}

	sort.Strings(names)

	return names, nil
}

// LabelNamesOffsetsFor decodes the offsets of the name symbols for a given series.
// They are returned in the same order they're stored, which should be sorted lexicographically.
func LabelNamesOffsets(d binaryreader.Decbuf) ([]uint32, error) {
	_ = d.Be64() // skip fingerprint
	k := d.Uvarint()

	offsets := make([]uint32, k)
	for i := 0; i < k; i++ {
		offsets[i] = uint32(d.Uvarint())
		_ = d.Uvarint() // skip the label value

		if d.Err() != nil {
			return nil, errors.Wrap(d.Err(), "read series label offsets")
		}
	}
	return offsets, d.Err()
}

// LabelValueFor returns label value for the given label name in the series referred to by ID.
func (r *StreamBinaryReader) LabelValueFor(id storage.SeriesRef, label string) (string, error) {
	var err error
	offset := id
	// In version 2+ series IDs are no longer exact references but series are 16-byte padded
	// and the ID is the multiple of 16 of the actual position.
	if r.version >= FormatV2 {
		offset = id * 16
	}
	d := r.factory.NewDecbufAtChecked(int(offset), castagnoliTable)
	defer runutil.CloseWithErrCapture(&err, &d, "label value for")
	value, err := LabelValueFor(d, r.dec, label)
	if err != nil || value == "" {
		return "", storage.ErrNotFound
	}
	return value, nil
}

// LabelValueFor decodes a label for a given series.
func LabelValueFor(d binaryreader.Decbuf, dec *Decoder, label string) (string, error) {
	_ = d.Be64() // skip fingerprint
	k := d.Uvarint()

	for i := 0; i < k; i++ {
		lno := uint32(d.Uvarint())
		lvo := uint32(d.Uvarint())

		if d.Err() != nil {
			return "", errors.Wrap(d.Err(), "read series label offsets")
		}

		ln, err := dec.LookupSymbol(lno)
		if err != nil {
			return "", errors.Wrap(err, "lookup label name")
		}

		if ln == label {
			lv, err := dec.LookupSymbol(lvo)
			if err != nil {
				return "", errors.Wrap(err, "lookup label value")
			}

			return lv, nil
		}
	}

	return "", d.Err()
}

// Series reads the series with the given ID and writes its labels and chunks into lbls and chks.
func (r *StreamBinaryReader) Series(id storage.SeriesRef, from int64, through int64, lbls *labels.Labels, chks *[]ChunkMeta) (uint64, error) {
	var err error
	offset := id
	// In version 2+ series IDs are no longer exact references but series are 16-byte padded
	// and the ID is the multiple of 16 of the actual position.
	if r.version >= FormatV2 {
		offset = id * 16
	}
	d := r.factory.NewDecbufUvarintAtChecked(int(offset), castagnoliTable)
	defer runutil.CloseWithErrCapture(&err, &d, "read series")
	if err = d.Err(); err != nil {
		return 0, err
	}

	fprint, err := r.dec.Series(r.version, d.Get(), id, from, through, lbls, chks)
	if err != nil {
		return 0, errors.Wrap(err, "read series")
	}
	return fprint, nil
}

func (r *StreamBinaryReader) ChunkStats(id storage.SeriesRef, from, through int64, lbls *labels.Labels) (uint64, ChunkStats, error) {
	var err error
	offset := id
	// In version 2+ series IDs are no longer exact references but series are 16-byte padded
	// and the ID is the multiple of 16 of the actual position.
	if r.version >= FormatV2 {
		offset = id * 16
	}
	d := r.factory.NewDecbufUvarintAtChecked(int(offset), castagnoliTable)
	defer runutil.CloseWithErrCapture(&err, &d, "read series")
	if err = d.Err(); err != nil {
		return 0, ChunkStats{}, err
	}

	return r.dec.ChunkStats(r.version, d.Get(), id, from, through, lbls)
}

func (r *StreamBinaryReader) Postings(name string, shard *ShardAnnotation, values ...string) (Postings, error) {
	var err error
	if r.version == FormatV1 {
		e, ok := r.postingsV1[name]
		if !ok {
			return EmptyPostings(), nil
		}
		res := make([]Postings, 0, len(values))
		for _, v := range values {
			postingsOff, ok := e[v]
			if !ok {
				continue
			}
			// Read from the postings table.
			d := r.factory.NewDecbufAtChecked(int(postingsOff), castagnoliTable)
			defer runutil.CloseWithErrCapture(&err, &d, "read postings")
			if err = d.Err(); err != nil {
				return nil, err
			}
			_, p, err2 := r.dec.Postings(d.Get())
			if err2 != nil {
				return nil, errors.Wrap(err, "decode postings")
			}
			res = append(res, p)
		}
		return Merge(res...), nil
	}

	e, ok := r.postings[name]
	if !ok {
		return EmptyPostings(), nil
	}

	if len(values) == 0 {
		return EmptyPostings(), nil
	}

	res := make([]Postings, 0, len(values))
	skip := 0
	valueIndex := 0
	for valueIndex < len(values) && values[valueIndex] < e[0].value {
		// Discard values before the start.
		valueIndex++
	}

	// Don't Crc32 the entire postings offset table, this is very slow
	// so hope any issues were caught at startup.
	d := r.factory.NewDecbufAtUnchecked(int(r.toc.PostingsTable))
	defer runutil.CloseWithErrCapture(&err, &d, "read posting")
	if err = d.Err(); err != nil {
		return nil, err
	}

	for valueIndex < len(values) {
		value := values[valueIndex]

		i := sort.Search(len(e), func(i int) bool { return e[i].value >= value })
		if i == len(e) {
			// We're past the end.
			break
		}
		if i > 0 && e[i].value != value {
			// Need to look from previous entry.
			i--
		}

		d.ResetAt(4 + e[i].off)

		// Iterate on the offset table.
		var postingsOff uint64 // The offset into the postings table.
		for d.Err() == nil {
			if skip == 0 {
				// These are always the same number of bytes,
				// and it's faster to skip than parse.
				skip = d.Len()
				d.Uvarint()            // Keycount.
				d.UnsafeUvarintBytes() // Label name.
				skip -= d.Len()
			} else {
				d.Skip(skip)
			}
			v := d.UvarintStr()
			postingsOff = d.Uvarint64() // Offset.
			for v >= value {
				if v == value {
					// Read from the postings table.
					d2 := r.factory.NewDecbufAtChecked(int(postingsOff), castagnoliTable)
					_, p, err := r.dec.Postings(d2.Get())
					err = d2.Close()
					if err != nil {
						return nil, errors.Wrap(err, "decode postings")
					}
					res = append(res, p)
				}
				valueIndex++
				if valueIndex == len(values) {
					break
				}
				value = values[valueIndex]
			}
			if i+1 == len(e) || value >= e[i+1].value || valueIndex == len(values) {
				// Need to go to a later postings offset entry, if there is one.
				break
			}
		}
		if d.Err() != nil {
			return nil, errors.Wrap(d.Err(), "get postings offset entry")
		}
	}

	merged := Merge(res...)
	if shard != nil {
		return NewShardedPostings(merged, *shard, r.fingerprintOffsets), nil
	}

	return merged, nil
}

// TODO(chaudum): Check if unused
func (r *StreamBinaryReader) Size() int64 {
	return 0
}

// LabelNames returns all the unique label names present in the index.
// TODO(twilkie) implement support for matchers
func (r *StreamBinaryReader) LabelNames(matchers ...*labels.Matcher) ([]string, error) {
	if len(matchers) > 0 {
		return nil, errors.Errorf("matchers parameter is not implemented: %+v", matchers)
	}

	labelNames := make([]string, 0, len(r.postings))
	for name := range r.postings {
		if name == allPostingsKey.Name {
			// This is not from any metric.
			continue
		}
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)
	return labelNames, nil
}
