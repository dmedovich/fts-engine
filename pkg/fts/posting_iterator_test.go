package fts

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type testIteratorIndex struct {
	data     map[string][]DocOrd
	opened   int
	closed   int
	openErr  error
	readErr  error
	closeErr error
}

type testCursor struct {
	ordinalSliceIterator
	index *testIteratorIndex
}

func (c *testCursor) Err() error { return c.index.readErr }

func (c *testCursor) Close() error {
	c.index.closed++
	return c.index.closeErr
}

func (i *testIteratorIndex) OpenPostings(_ context.Context, key string) (PostingIterator, uint64, error) {
	if i.openErr != nil {
		return nil, 0, i.openErr
	}
	i.opened++
	c := &testCursor{
		ordinalSliceIterator: ordinalSliceIterator{ords: i.data[key], pos: -1},
		index:                i,
	}
	return c, uint64(len(i.data[key])), nil
}

func TestIntersectTermsContract(t *testing.T) {
	for _, tc := range []struct {
		name       string
		keys       []string
		candidates []DocOrd
		want       []DocOrd
		invalid    bool
	}{
		{"all", []string{"a", "b"}, nil, []DocOrd{0, 4, 9}, false},
		{"filtered", []string{"a", "b"}, []DocOrd{1, 4, 8}, []DocOrd{4}, false},
		{"empty", []string{"a"}, []DocOrd{}, nil, false},
		{"no_keys", nil, nil, nil, false},
		{"missing", []string{"a", "missing"}, nil, nil, false},
		{"duplicates", []string{"a", "a", "b"}, nil, []DocOrd{0, 4, 9}, false},
		{"unsorted", []string{"a"}, []DocOrd{4, 0}, nil, true},
		{"duplicate_candidates", []string{"a"}, []DocOrd{4, 4}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			i := &testIteratorIndex{data: map[string][]DocOrd{"a": {0, 1, 4, 9}, "b": {0, 4, 8, 9}}}
			var got []DocOrd
			err := IntersectTerms(context.Background(), i, tc.keys, tc.candidates, func(o DocOrd) bool {
				got = append(got, o)
				return true
			})
			if (err != nil) != tc.invalid || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, %v; want %v invalid=%v", got, err, tc.want, tc.invalid)
			}
			if i.opened != i.closed {
				t.Fatalf("leaked %d cursors", i.opened-i.closed)
			}
		})
	}
}

func TestIntersectTermsLifecycle(t *testing.T) {
	sentinel := errors.New("injected")
	for _, stage := range []string{"open", "read", "close", "stop", "cancel"} {
		t.Run(stage, func(t *testing.T) {
			i := &testIteratorIndex{data: map[string][]DocOrd{"a": {0, 1, 2}}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch stage {
			case "open":
				i.openErr = sentinel
			case "read":
				i.readErr = sentinel
			case "close":
				i.closeErr = sentinel
			}
			calls := 0
			err := IntersectTerms(ctx, i, []string{"a"}, nil, func(DocOrd) bool {
				calls++
				if stage == "cancel" {
					cancel()
				}
				return stage != "stop"
			})
			switch stage {
			case "stop":
				if err != nil || calls != 1 {
					t.Fatalf("stop: calls=%d err=%v", calls, err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			default:
				if !errors.Is(err, sentinel) {
					t.Fatal(err)
				}
			}
			if i.opened != i.closed {
				t.Fatalf("leaked cursors: %d/%d", i.closed, i.opened)
			}
		})
	}
}

func TestStreamingCandidatesOwnership(t *testing.T) {
	readErr, closeErr := errors.New("read"), errors.New("close")
	for _, stage := range []string{"nil_index", "nil_yield", "no_keys", "empty", "open", "missing", "stop", "read", "cancel"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			i := &testIteratorIndex{data: map[string][]DocOrd{"a": {0, 1, 2}}}
			owner := &testIteratorIndex{}
			candidate := &testCursor{
				ordinalSliceIterator: ordinalSliceIterator{ords: []DocOrd{0, 2}, pos: -1},
				index:                owner,
			}
			var idx PostingIteratorIndex = i
			keys, count := []string{"a"}, uint64(2)
			yield := func(DocOrd) bool { return stage != "stop" }
			switch stage {
			case "nil_index":
				idx = nil
			case "nil_yield":
				yield = nil
			case "no_keys":
				keys = nil
			case "empty":
				count = 0
			case "open":
				i.openErr = readErr
			case "missing":
				keys = []string{"a", "absent"}
			case "read":
				owner.readErr, owner.closeErr = readErr, closeErr
			case "cancel":
				cancel()
			}
			err := IntersectTermsWithIterator(ctx, idx, keys, candidate, count, yield)
			if owner.closed != 1 || i.opened != i.closed {
				t.Fatalf("leaked cursors: candidate=%d index=%d/%d", owner.closed, i.closed, i.opened)
			}
			if stage == "read" && (!errors.Is(err, readErr) || !errors.Is(err, closeErr)) {
				t.Fatal(err)
			}
			if stage == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}

// External cursors need not implement context cancellation themselves. In
// particular, cancellation by the last callback must not look like completion.
func TestCancellationAtLastResult(t *testing.T) {
	i := &testIteratorIndex{data: map[string][]DocOrd{"a": {0}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := IntersectTerms(ctx, i, []string{"a"}, nil, func(DocOrd) bool {
		cancel()
		return true
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
