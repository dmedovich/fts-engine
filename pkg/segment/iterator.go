package segment

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

// OpenPostings exposes existing ranked segments to unranked intersection without
// materializing their full posting lists. Advance decodes sequentially in this
// format; presence segments additionally support block skipping.
func (r *Reader) OpenPostings(ctx context.Context, key string) (fts.PostingIterator, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	c := &postingCursor{ctx: ctx}
	e, ok := r.findTerm(key)
	if !ok {
		c.done = true
		return c, 0, nil
	}
	c.input = bufio.NewReaderSize(io.NewSectionReader(r.readerAt, int64(r.postingsBase+e.postingsOff), int64(e.postingsLen)), 4096)
	count, err := binary.ReadUvarint(c.input)
	if err != nil || count == 0 || count > e.postingsLen/2 {
		return nil, 0, fmt.Errorf("segment: invalid posting count: %w", errOrInvalid(err))
	}
	c.remaining = count
	return c, count, nil
}

func errOrInvalid(err error) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("invalid value")
}

type postingCursor struct {
	ctx       context.Context
	input     *bufio.Reader
	remaining uint64
	ord       fts.DocOrd
	valid     bool
	done      bool
	err       error
}

func (c *postingCursor) Ord() fts.DocOrd { return c.ord }

func (c *postingCursor) Err() error { return c.err }

func (c *postingCursor) Close() error {
	c.done = true
	c.input = nil
	return nil
}

func (c *postingCursor) Next() bool {
	if c.done {
		return false
	}
	if err := c.ctx.Err(); err != nil {
		c.err = err
		c.done = true
		return false
	}
	if c.remaining == 0 {
		c.done = true
		return false
	}
	delta, err := binary.ReadUvarint(c.input)
	if err != nil || delta > uint64(math.MaxUint32-uint32(c.ord)) || c.valid && delta == 0 {
		c.err = fmt.Errorf("segment: invalid posting ordinal: %w", errOrInvalid(err))
		c.done = true
		return false
	}
	freq, err := binary.ReadUvarint(c.input)
	if err != nil || freq == 0 || freq > math.MaxUint32 {
		c.err = fmt.Errorf("segment: invalid posting frequency: %w", errOrInvalid(err))
		c.done = true
		return false
	}
	c.ord += fts.DocOrd(delta)
	c.valid = true
	c.remaining--
	if c.remaining == 0 {
		_, err := c.input.ReadByte()
		if err != io.EOF {
			if err == nil {
				err = fmt.Errorf("trailing posting data")
			}
			c.err = err
			c.done = true
			return false
		}
	}
	return true
}

func (c *postingCursor) Advance(target fts.DocOrd) bool {
	if c.done {
		return false
	}
	if err := c.ctx.Err(); err != nil {
		c.err = err
		c.done = true
		return false
	}
	if c.valid && c.ord >= target {
		return true
	}
	for c.Next() {
		if c.ord >= target {
			return true
		}
	}
	return false
}

var _ fts.PostingIteratorIndex = (*Reader)(nil)
