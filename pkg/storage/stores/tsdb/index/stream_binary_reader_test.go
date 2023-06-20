package index

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestStreamBinaryReader_Open(t *testing.T) {
	dir := t.TempDir()

	fn := filepath.Join(dir, IndexFilename)

	// An empty index must still result in a readable file.
	iw, err := NewWriter(context.Background(), fn)
	require.NoError(t, err)
	require.NoError(t, iw.Close())

	ir, err := NewStreamBinaryReader(fn)
	require.NoError(t, err)
	require.NoError(t, ir.Close())

	// Modify magic header must cause open to fail.
	f, err := os.OpenFile(fn, os.O_WRONLY, 0o666)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0, 0}, 0)
	require.NoError(t, err)
	f.Close()

	_, err = NewStreamBinaryReader(fn)
	require.Error(t, err)
}

func TestStreamBinaryReader_Postings(t *testing.T) {
	dir := t.TempDir()

	fn := filepath.Join(dir, IndexFilename)

	iw, err := NewWriter(context.Background(), fn)
	require.NoError(t, err)

	series := []labels.Labels{
		labels.FromStrings("a", "1", "b", "1"),
		labels.FromStrings("a", "1", "b", "2"),
		labels.FromStrings("a", "1", "b", "3"),
		labels.FromStrings("a", "1", "b", "4"),
	}

	require.NoError(t, iw.AddSymbol("1"))
	require.NoError(t, iw.AddSymbol("2"))
	require.NoError(t, iw.AddSymbol("3"))
	require.NoError(t, iw.AddSymbol("4"))
	require.NoError(t, iw.AddSymbol("a"))
	require.NoError(t, iw.AddSymbol("b"))

	// Postings lists are only written if a series with the respective
	// reference was added before.
	require.NoError(t, iw.AddSeries(1, series[0], model.Fingerprint(series[0].Hash())))
	require.NoError(t, iw.AddSeries(2, series[1], model.Fingerprint(series[1].Hash())))
	require.NoError(t, iw.AddSeries(3, series[2], model.Fingerprint(series[2].Hash())))
	require.NoError(t, iw.AddSeries(4, series[3], model.Fingerprint(series[3].Hash())))

	require.NoError(t, iw.Close())

	ir, err := NewStreamBinaryReader(fn)
	require.NoError(t, err)

	p, err := ir.Postings("a", nil, "1")
	require.NoError(t, err)

	var l labels.Labels
	var c []ChunkMeta

	for i := 0; p.Next(); i++ {
		_, err := ir.Series(p.At(), 0, math.MaxInt64, &l, &c)

		require.NoError(t, err)
		require.Equal(t, 0, len(c))
		require.Equal(t, series[i], l)
	}
	require.NoError(t, p.Err())

	// The label indices are no longer used, so test them by hand here.
	labelIndices := map[string][]string{}
	require.NoError(t, BinaryReadOffsetTable(ir.factory, ir.toc.LabelIndicesTable, func(key []string, off uint64, _ int) error {
		if len(key) != 1 {
			return errors.Errorf("unexpected key length for label indices table %d", len(key))
		}

		d := ir.factory.NewDecbufAtChecked(int(off), castagnoliTable)
		vals := []string{}
		nc := d.Be32int()
		if nc != 1 {
			return errors.Errorf("unexpected number of label indices table names %d", nc)
		}
		for i := d.Be32(); i > 0; i-- {
			v, err := ir.lookupSymbol(d.Be32())
			if err != nil {
				return err
			}
			vals = append(vals, v)
		}
		labelIndices[key[0]] = vals
		return d.Err()
	}))
	require.Equal(t, map[string][]string{
		"a": {"1"},
		"b": {"1", "2", "3", "4"},
	}, labelIndices)

	require.NoError(t, ir.Close())
}

func TestStreamBinaryReader_PostingsMany(t *testing.T) {
	dir := t.TempDir()

	fn := filepath.Join(dir, IndexFilename)

	iw, err := NewWriter(context.Background(), fn)
	require.NoError(t, err)

	// Create a label in the index which has 999 values.
	symbols := map[string]struct{}{}
	series := []labels.Labels{}
	for i := 1; i < 1000; i++ {
		v := fmt.Sprintf("%03d", i)
		series = append(series, labels.FromStrings("i", v, "foo", "bar"))
		symbols[v] = struct{}{}
	}
	symbols["i"] = struct{}{}
	symbols["foo"] = struct{}{}
	symbols["bar"] = struct{}{}
	syms := []string{}
	for s := range symbols {
		syms = append(syms, s)
	}
	sort.Strings(syms)
	for _, s := range syms {
		require.NoError(t, iw.AddSymbol(s))
	}

	sort.Slice(series, func(i, j int) bool {
		return series[i].Hash() < series[j].Hash()
	})

	for i, s := range series {
		require.NoError(t, iw.AddSeries(storage.SeriesRef(i), s, model.Fingerprint(s.Hash())))
	}
	require.NoError(t, iw.Close())

	ir, err := NewStreamBinaryReader(fn)
	require.NoError(t, err)
	defer func() { require.NoError(t, ir.Close()) }()

	cases := []struct {
		in []string
	}{
		// Simple cases, everything is present.
		{in: []string{"002"}},
		{in: []string{"031", "032", "033"}},
		{in: []string{"032", "033"}},
		{in: []string{"127", "128"}},
		{in: []string{"127", "128", "129"}},
		{in: []string{"127", "129"}},
		{in: []string{"128", "129"}},
		{in: []string{"998", "999"}},
		{in: []string{"999"}},
		// Before actual values.
		{in: []string{"000"}},
		{in: []string{"000", "001"}},
		{in: []string{"000", "002"}},
		// After actual values.
		{in: []string{"999a"}},
		{in: []string{"999", "999a"}},
		{in: []string{"998", "999", "999a"}},
		// In the middle of actual values.
		{in: []string{"126a", "127", "128"}},
		{in: []string{"127", "127a", "128"}},
		{in: []string{"127", "127a", "128", "128a", "129"}},
		{in: []string{"127", "128a", "129"}},
		{in: []string{"128", "128a", "129"}},
		{in: []string{"128", "129", "129a"}},
		{in: []string{"126a", "126b", "127", "127a", "127b", "128", "128a", "128b", "129", "129a", "129b"}},
	}

	for idx, c := range cases {
		t.Run(fmt.Sprint(idx), func(t *testing.T) {
			it, err := ir.Postings("i", nil, c.in...)
			require.NoError(t, err)

			got := []string{}
			var lbls labels.Labels
			var metas []ChunkMeta
			for it.Next() {
				_, err := ir.Series(it.At(), 0, math.MaxInt64, &lbls, &metas)
				require.NoError(t, err)
				got = append(got, lbls.Get("i"))
			}
			require.NoError(t, it.Err())
			exp := []string{}
			for _, e := range c.in {
				if _, ok := symbols[e]; ok && e != "l" {
					exp = append(exp, e)
				}
			}

			// sort expected values by label hash instead of lexicographically by labelset
			sort.Slice(exp, func(i, j int) bool {
				return labels.FromStrings("i", exp[i], "foo", "bar").Hash() < labels.FromStrings("i", exp[j], "foo", "bar").Hash()
			})

			require.Equal(t, exp, got, fmt.Sprintf("input: %v", c.in))
		})
	}
}

func TestStreamBinaryReader_Persistence_Index_e2e(t *testing.T) {
	dir := t.TempDir()

	lbls, err := labels.ReadLabels(filepath.Join("..", "testdata", "20kseries.json"), 20000)
	require.NoError(t, err)

	// Sort labels as the index writer expects series in sorted order by fingerprint.
	sort.Slice(lbls, func(i, j int) bool {
		return lbls[i].Hash() < lbls[j].Hash()
	})

	symbols := map[string]struct{}{}
	for _, lset := range lbls {
		for _, l := range lset {
			symbols[l.Name] = struct{}{}
			symbols[l.Value] = struct{}{}
		}
	}

	var input indexWriterSeriesSlice

	// Generate ChunkMetas for every label set.
	for i, lset := range lbls {
		var metas []ChunkMeta

		for j := 0; j <= (i % 20); j++ {
			metas = append(metas, ChunkMeta{
				MinTime:  int64(j * 10000),
				MaxTime:  int64((j + 1) * 10000),
				Checksum: rand.Uint32(),
			})
		}
		input = append(input, &indexWriterSeries{
			labels: lset,
			chunks: metas,
		})
	}

	iw, err := NewWriter(context.Background(), filepath.Join(dir, IndexFilename))
	require.NoError(t, err)

	syms := []string{}
	for s := range symbols {
		syms = append(syms, s)
	}
	sort.Strings(syms)
	for _, s := range syms {
		require.NoError(t, iw.AddSymbol(s))
	}

	// Population procedure as done by compaction.
	var (
		postings = NewMemPostings()
		values   = map[string]map[string]struct{}{}
	)

	mi := newMockIndex()

	for i, s := range input {
		err = iw.AddSeries(storage.SeriesRef(i), s.labels, model.Fingerprint(s.labels.Hash()), s.chunks...)
		require.NoError(t, err)
		require.NoError(t, mi.AddSeries(storage.SeriesRef(i), s.labels, s.chunks...))

		for _, l := range s.labels {
			valset, ok := values[l.Name]
			if !ok {
				valset = map[string]struct{}{}
				values[l.Name] = valset
			}
			valset[l.Value] = struct{}{}
		}
		postings.Add(storage.SeriesRef(i), s.labels)
	}

	err = iw.Close()
	require.NoError(t, err)

	ir, err := NewStreamBinaryReader(filepath.Join(dir, IndexFilename))
	require.NoError(t, err)

	for p := range mi.postings {
		gotp, err := ir.Postings(p.Name, nil, p.Value)
		require.NoError(t, err)

		expp, err := mi.Postings(p.Name, p.Value)
		require.NoError(t, err)

		var lset, explset labels.Labels
		var chks, expchks []ChunkMeta

		for gotp.Next() {
			require.True(t, expp.Next())

			ref := gotp.At()

			_, err := ir.Series(ref, 0, math.MaxInt64, &lset, &chks)
			require.NoError(t, err)

			err = mi.Series(expp.At(), &explset, &expchks)
			require.NoError(t, err)
			require.Equal(t, explset, lset)
			require.Equal(t, expchks, chks)
		}
		require.False(t, expp.Next(), "Expected no more postings for %q=%q", p.Name, p.Value)
		require.NoError(t, gotp.Err())
	}

	labelPairs := map[string][]string{}
	for l := range mi.postings {
		labelPairs[l.Name] = append(labelPairs[l.Name], l.Value)
	}
	for k, v := range labelPairs {
		sort.Strings(v)

		res, err := ir.SortedLabelValues(k)
		require.NoError(t, err)

		require.Equal(t, len(v), len(res))
		for i := 0; i < len(v); i++ {
			require.Equal(t, v[i], res[i])
		}
	}

	gotSymbols := []string{}
	it := ir.Symbols()
	for it.Next() {
		gotSymbols = append(gotSymbols, it.At())
	}
	require.NoError(t, it.Err())
	expSymbols := []string{}
	for s := range mi.symbols {
		expSymbols = append(expSymbols, s)
	}
	sort.Strings(expSymbols)
	require.Equal(t, len(expSymbols), len(gotSymbols))
	require.Equal(t, expSymbols, gotSymbols)

	require.NoError(t, ir.Close())
}
