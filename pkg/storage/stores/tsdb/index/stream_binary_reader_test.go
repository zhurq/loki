// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"context"
	"fmt"
	"math"
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

	for _, c := range cases {
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
	}
}
