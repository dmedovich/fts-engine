package presence

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

func benchmarkSource() source {
	s := source{{Term: "common"}, {Term: "half"}, {Term: "rare"}}
	for o := range fts.DocOrd(100000) {
		p := fts.Posting{Ord: o, Count: 1}
		s[0].Postings = append(s[0].Postings, p)
		if o%2 == 0 {
			s[1].Postings = append(s[1].Postings, p)
		}
		if o%1000 == 0 {
			s[2].Postings = append(s[2].Postings, p)
		}
	}
	return s
}

// This baseline is the existing slice-returning sealed API followed by sorted
// intersection and external candidate filtering. All paths stream the same final
// ordinals to the same callback, so result allocation is not charged selectively.
func materialized(ctx context.Context, r *segment.Reader, keys []string, candidates []fts.DocOrd, yield func(fts.DocOrd) bool) error {
	var acc []fts.Posting
	for i, key := range keys {
		p, err := r.Search(key)
		if err != nil {
			return err
		}
		if i == 0 {
			acc = p
			continue
		}
		out := acc[:0]
		a, b := 0, 0
		for a < len(acc) && b < len(p) {
			if acc[a].Ord == p[b].Ord {
				out = append(out, acc[a])
				a++
				b++
			} else if acc[a].Ord < p[b].Ord {
				a++
			} else {
				b++
			}
		}
		acc = out
	}
	j := 0
	for _, p := range acc {
		if err := ctx.Err(); err != nil {
			return err
		}
		if candidates != nil {
			for j < len(candidates) && candidates[j] < p.Ord {
				j++
			}
			if j == len(candidates) {
				break
			}
			if candidates[j] != p.Ord {
				continue
			}
		}
		if !yield(p.Ord) {
			break
		}
	}
	return nil
}

func BenchmarkIntersection(b *testing.B) {
	s := benchmarkSource()
	regular, err := segment.Build(s)
	if err != nil {
		b.Fatal(err)
	}
	compact, err := BuildFromSource(s)
	if err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		keys       []string
		candidates []fts.DocOrd
		want       int
	}{
		{"all", []string{"common", "half"}, nil, 50000},
		{"single", []string{"common"}, nil, 100000},
		{"missing", []string{"common", "missing"}, nil, 0},
		{"rare", []string{"common", "rare"}, nil, 100},
		{"candidates", []string{"common", "half"}, []fts.DocOrd{0, 10000, 50000, 99998}, 4},
	} {
		for _, mode := range []string{"materialized", "segment_iterator", "presence"} {
			b.Run(tc.name+"/"+mode, func(b *testing.B) {
				data := regular
				if mode == "presence" {
					data = compact
				}
				backing := &countedReader{Reader: bytes.NewReader(data)}
				var index fts.PostingIteratorIndex
				var old *segment.Reader
				if mode == "presence" {
					index, err = OpenReaderAt(backing, int64(len(data)))
				} else {
					old, err = segment.OpenReaderAt(backing, int64(len(data)))
					index = old
				}
				if err != nil {
					b.Fatal(err)
				}
				backing.calls = 0
				backing.read = 0
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					count := 0
					yield := func(fts.DocOrd) bool {
						count++
						return true
					}
					if mode == "materialized" {
						err = materialized(context.Background(), old, tc.keys, tc.candidates, yield)
					} else {
						err = fts.IntersectTerms(context.Background(), index, tc.keys, tc.candidates, yield)
					}
					if err != nil || count != tc.want {
						b.Fatalf("count=%d want=%d err=%v", count, tc.want, err)
					}
				}
				b.ReportMetric(float64(backing.read)/float64(b.N), "read-B/op")
				b.ReportMetric(float64(backing.calls)/float64(b.N), "reads/op")
				b.ReportMetric(float64(len(data)), "index-B")
			})
		}
	}
}

func BenchmarkBuild(b *testing.B) {
	for _, kind := range []string{"frequent", "singletons"} {
		s := benchmarkSource()
		if kind == "singletons" {
			s = make(source, 100000)
			for i := range s {
				s[i] = segment.TermPostings{Term: fmt.Sprintf("trace-%032x", i), Postings: postings(fts.DocOrd(i))}
			}
		}
		for _, mode := range []string{"segment", "presence", "presence_stream"} {
			b.Run(kind+"/"+mode, func(b *testing.B) {
				var data []byte
				var err error
				encoded, err := BuildFromSource(s)
				if err != nil {
					b.Fatal(err)
				}
				size := len(encoded)
				b.ReportAllocs()
				for b.Loop() {
					switch mode {
					case "segment":
						data, err = segment.Build(s)
					case "presence_stream":
						err = WriteFromSource(context.Background(), io.Discard, s)
					default:
						data, err = BuildFromSource(s)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				if mode == "presence_stream" {
					b.ReportMetric(float64(size), "index-B")
				} else {
					b.ReportMetric(float64(len(data)), "index-B")
				}
			})
		}
	}
}

// Includes candidate validation, lazy ID-to-ordinal conversion, and final ID
// resolution. IDs are sparse, and the input includes IDs outside the segment.
func BenchmarkExternalIDs(b *testing.B) {
	blob, err := BuildFromSource(benchmarkSource())
	if err != nil {
		b.Fatal(err)
	}
	r, err := Open(blob)
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]uint64, 100000)
	for i := range ids {
		ids[i] = uint64(i) * 1000000007
	}
	table, err := fts.NewOrdinalTable(ids)
	if err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		candidates []uint64
		keys       []string
		want       int
	}{
		{"sparse", []uint64{0, 1, ids[10000], ids[50000], ids[99998], ids[99998] + 1}, []string{"common", "half"}, 4},
		{"dense", ids, []string{"common", "half"}, 50000},
		{"dense_rare", ids, []string{"common", "rare"}, 100},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				c, n, err := table.Candidates(context.Background(), tc.candidates)
				if err != nil {
					b.Fatal(err)
				}
				count := 0
				err = fts.IntersectTermsWithIterator(context.Background(), r, tc.keys, c, n, func(o fts.DocOrd) bool {
					if _, ok := table.ID(o); !ok {
						b.Fatal("invalid ordinal")
					}
					count++
					return true
				})
				if err != nil || count != tc.want {
					b.Fatalf("count=%d err=%v", count, err)
				}
			}
		})
	}
}
