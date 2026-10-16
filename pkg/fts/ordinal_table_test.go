package fts

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"slices"
	"testing"
)

func TestOrdinalTable(t *testing.T) {
	ids := []uint64{0, 42, 1000000000001, math.MaxUint64}
	table, err := NewOrdinalTable(ids)
	if err != nil {
		t.Fatal(err)
	}
	if table.Len() != len(ids) {
		t.Fatal(table.Len())
	}
	for i, id := range ids {
		if got, ok := table.ID(DocOrd(i)); !ok || got != id {
			t.Fatalf("ID(%d) = %d, %v", i, got, ok)
		}
		if got, ok := table.Ordinal(id); !ok || got != DocOrd(i) {
			t.Fatalf("Ordinal(%d) = %d, %v", id, got, ok)
		}
	}
	if _, ok := table.ID(math.MaxUint32); ok {
		t.Fatal("out of range ordinal")
	}
	if _, ok := table.Ordinal(1); ok {
		t.Fatal("absent ID")
	}
	for _, bad := range [][]uint64{{1, 0}, {1, 1}} {
		if _, err := NewOrdinalTable(bad); err == nil {
			t.Fatal("accepted invalid table")
		}
		if _, _, err := table.Candidates(context.Background(), bad); err == nil {
			t.Fatal("accepted invalid candidates")
		}
	}
	for _, ids := range [][]uint64{nil, {}, {0, 1, 42, math.MaxUint64}} {
		c, n, err := table.Candidates(context.Background(), ids)
		if err != nil {
			t.Fatal(err)
		}
		if ids == nil {
			if c != nil {
				t.Fatal("nil must be unrestricted")
			}
			continue
		}
		var got []DocOrd
		for c.Next() {
			got = append(got, c.Ord())
		}
		if c.Err() != nil || uint64(len(got)) > n {
			t.Fatalf("%v %v", got, c.Err())
		}
		if len(ids) > 0 && !slices.Equal(got, []DocOrd{0, 1, 3}) {
			t.Fatal(got)
		}
		if c.Advance(0) || c.Next() {
			t.Fatal("exhaustion not permanent")
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c, _, err := table.Candidates(ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if c.Next() || !errors.Is(c.Err(), context.Canceled) {
		t.Fatal(c.Err())
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := table.Candidates(ctx, ids); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestIDCandidatesRandomTransitions(t *testing.T) {
	rng := rand.New(rand.NewSource(8219))
	for trial := range 300 {
		ids, filter := make([]uint64, 0), make([]uint64, 0)
		for i := range 1000 {
			id := uint64(i) * 1000000007
			if rng.Intn(3) == 0 {
				ids = append(ids, id)
			}
			if rng.Intn(5) == 0 {
				filter = append(filter, id)
			}
		}
		table, err := NewOrdinalTable(ids)
		if err != nil {
			t.Fatal(err)
		}
		var want []DocOrd
		for i, id := range ids {
			if slices.Contains(filter, id) {
				want = append(want, DocOrd(i))
			}
		}
		c, _, err := table.Candidates(context.Background(), filter)
		if err != nil {
			t.Fatal(err)
		}
		ref := &ordinalSliceIterator{ords: want, pos: -1}
		for range 150 {
			var gotValid, wantValid bool
			if rng.Intn(3) == 0 {
				gotValid, wantValid = c.Next(), ref.Next()
			} else {
				target := DocOrd(rng.Intn(len(ids) + 10))
				gotValid, wantValid = c.Advance(target), ref.Advance(target)
			}
			if gotValid != wantValid || c.Err() != nil {
				t.Fatalf("trial %d: valid=%v, want %v; error=%v", trial, gotValid, wantValid, c.Err())
			}
			if gotValid && c.Ord() != ref.Ord() {
				t.Fatalf("trial %d: ordinal=%d, want %d", trial, c.Ord(), ref.Ord())
			}
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if c.Next() || c.Advance(0) {
			t.Fatal("closed cursor advanced")
		}
	}
}

func TestIDCandidatesIntersection(t *testing.T) {
	table, err := NewOrdinalTable([]uint64{0, 100, 200, math.MaxUint64})
	if err != nil {
		t.Fatal(err)
	}
	i := &testIteratorIndex{data: map[string][]DocOrd{"a": {0, 1, 2, 3}, "b": {1, 3}}}
	for _, ids := range [][]uint64{nil, {}, {0, 99, 100, 200, math.MaxUint64}, {99}} {
		c, count, err := table.Candidates(context.Background(), ids)
		if err != nil {
			t.Fatal(err)
		}
		var got []uint64
		err = IntersectTermsWithIterator(context.Background(), i, []string{"a", "b"}, c, count, func(o DocOrd) bool {
			id, ok := table.ID(o)
			if !ok {
				t.Fatalf("ID(%d): ordinal out of range", o)
			}
			got = append(got, id)
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		want := []uint64{100, math.MaxUint64}
		if ids != nil && len(ids) <= 1 {
			want = nil
		}
		if !slices.Equal(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}
