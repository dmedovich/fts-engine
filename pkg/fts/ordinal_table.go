package fts

import (
	"context"
	"fmt"
	"math"
	"sort"
)

// OrdinalTable maps dense segment ordinals to sorted external uint64 IDs.
// It has no analyzer or persistence policy. The embedding store must persist
// the exact table used to assign ordinals with the matching segment. Do not
// reorder or drop entries after indexing, including documents with no tokens.
// An immutable table is safe for concurrent queries.
type OrdinalTable struct {
	ids []uint64
}

// NewOrdinalTable validates and borrows ids without copying them. IDs must be
// strictly increasing and must not be mutated during the table's lifetime.
// Ordinal zero maps to ids[0]; zero and MaxUint64 are both valid external IDs.
func NewOrdinalTable(ids []uint64) (*OrdinalTable, error) {
	if uint64(len(ids)) > uint64(math.MaxUint32)+1 {
		return nil, fmt.Errorf("fts: ordinal table exceeds uint32 address space")
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			return nil, fmt.Errorf("fts: IDs must be strictly increasing")
		}
	}
	return &OrdinalTable{ids: ids}, nil
}

// Len returns the number of document IDs in the table.
func (t *OrdinalTable) Len() int { return len(t.ids) }

// ID resolves an ordinal. The boolean is false for an out-of-range ordinal.
func (t *OrdinalTable) ID(ord DocOrd) (uint64, bool) {
	if uint64(ord) >= uint64(len(t.ids)) {
		return 0, false
	}
	return t.ids[ord], true
}

// Ordinal resolves an external ID without an auxiliary hash map.
func (t *OrdinalTable) Ordinal(id uint64) (DocOrd, bool) {
	i := sort.Search(len(t.ids), func(i int) bool { return t.ids[i] >= id })
	if i == len(t.ids) || t.ids[i] != id {
		return 0, false
	}
	return DocOrd(i), true
}

// Candidates lazily translates sorted external IDs to ordinals, ignoring IDs
// absent from this segment. It returns a cursor and a count upper bound suitable
// for IntersectTermsWithIterator, without allocating an ordinal slice. Nil IDs
// mean unrestricted (nil cursor); a non-nil empty slice means no matches.
// IDs must be strictly increasing and immutable until the cursor is closed.
func (t *OrdinalTable) Candidates(ctx context.Context, ids []uint64) (PostingIterator, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if ids == nil {
		return nil, 0, nil
	}
	for i := 1; i < len(ids); i++ {
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, 0, err
			}
		}
		if ids[i] <= ids[i-1] {
			return nil, 0, fmt.Errorf("fts: candidate IDs must be strictly increasing")
		}
	}
	c := &idCandidateIterator{
		ctx:   ctx,
		table: t.ids,
		ids:   ids,
		pos:   -1,
	}
	return c, uint64(min(len(ids), len(t.ids))), nil
}

type idCandidateIterator struct {
	ctx     context.Context
	table   []uint64
	ids     []uint64
	pos     int
	nextOrd int
	ord     DocOrd
	valid   bool
	done    bool
	err     error
}

func (c *idCandidateIterator) Ord() DocOrd { return c.ord }

func (c *idCandidateIterator) Err() error { return c.err }

func (c *idCandidateIterator) Close() error {
	c.done = true
	c.table, c.ids = nil, nil
	return nil
}

func (c *idCandidateIterator) ready() bool {
	if c.done {
		return false
	}
	if err := c.ctx.Err(); err != nil {
		c.err = err
		c.done = true
		return false
	}
	return true
}

func (c *idCandidateIterator) Next() bool {
	if !c.ready() {
		return false
	}
	c.pos++
	for c.pos < len(c.ids) && c.nextOrd < len(c.table) {
		if !c.ready() {
			return false
		}
		id := c.ids[c.pos]
		start := c.nextOrd
		if c.table[start] < id {
			c.nextOrd = seekID(c.table, start, id)
		}
		if c.nextOrd == len(c.table) {
			break
		}
		if c.table[c.nextOrd] == id {
			c.ord = DocOrd(c.nextOrd)
			c.nextOrd++
			c.valid = true
			return true
		}
		start = c.pos + 1
		c.pos = seekID(c.ids, start, c.table[c.nextOrd])
	}
	c.done = true
	return false
}

func (c *idCandidateIterator) Advance(target DocOrd) bool {
	if !c.ready() {
		return false
	}
	if c.valid && c.ord >= target {
		return true
	}
	if uint64(target) >= uint64(len(c.table)) {
		c.done = true
		return false
	}
	c.nextOrd = max(c.nextOrd, int(target))
	start := c.pos + 1
	if start < len(c.ids) && c.ids[start] < c.table[target] {
		c.pos = seekID(c.ids, start, c.table[target]) - 1
	}
	return c.Next()
}

// Galloping bounds make dense candidate traversal linear, while distant sparse
// probes still take logarithmic comparisons in the size of the skipped gap.
func seekID(ids []uint64, start int, target uint64) int {
	if start == len(ids) || ids[start] >= target {
		return start
	}
	span, remaining := 1, len(ids)-start
	for span < remaining && ids[start+span] < target {
		span += min(span, remaining-span)
	}
	return start + sort.Search(span, func(i int) bool { return ids[start+i] >= target })
}
