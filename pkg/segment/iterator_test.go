package segment_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

type failingReader struct {
	*bytes.Reader
	fail bool
}

var errIteratorRead = errors.New("injected I/O error")

func (r *failingReader) ReadAt(p []byte, off int64) (int, error) {
	if r.fail {
		return 0, errIteratorRead
	}
	return r.Reader.ReadAt(p, off)
}

func TestPostingIteratorLifecycle(t *testing.T) {
	blob, err := segment.Build([]segment.TermPostings{{Term: "a", Postings: []fts.Posting{{Ord: 0, Count: 2}, {Ord: 7, Count: 1}, {Ord: math.MaxUint32, Count: 3}}}})
	if err != nil {
		t.Fatal(err)
	}
	backing := &failingReader{Reader: bytes.NewReader(blob)}
	r, err := segment.OpenReaderAt(backing, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, n, err := r.OpenPostings(ctx, "a")
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	defer c.Close()
	if !c.Advance(0) || c.Ord() != 0 || !c.Next() || c.Ord() != 7 || !c.Advance(1) || c.Ord() != 7 {
		t.Fatal("bad cursor transitions")
	}
	if !c.Advance(math.MaxUint32) || c.Ord() != math.MaxUint32 || c.Next() || c.Advance(0) || c.Err() != nil {
		t.Fatalf("bad exhaustion: %v", c.Err())
	}
	c, _, err = r.OpenPostings(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cancel()
	if c.Next() || !errors.Is(c.Err(), context.Canceled) {
		t.Fatalf("cancel: %v", c.Err())
	}
	if _, _, err = r.OpenPostings(ctx, "a"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	c, n, err = r.OpenPostings(context.Background(), "missing")
	if err != nil || n != 0 || c.Next() || c.Err() != nil {
		t.Fatal("missing key")
	}
	_ = c.Close()
	backing.fail = true
	if _, _, err = r.OpenPostings(context.Background(), "a"); !errors.Is(err, errIteratorRead) {
		t.Fatalf("I/O error lost: %v", err)
	}
}
