package fts

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
)

// PostingIterator enumerates strictly increasing ordinals. It starts before the
// first item. Advance moves to the first ordinal >= target, retaining the current
// item if it already satisfies target. Exhaustion is permanent. Ord is valid only
// after a successful Next/Advance. Check Err after either returns false.
// Iterators are not safe for concurrent use and must be closed, even on errors.
type PostingIterator interface {
	Next() bool
	Advance(target DocOrd) bool
	Ord() DocOrd
	Err() error
	Close() error
}

// PostingIteratorIndex is an optional, unranked exact-key capability. It is
// independent of Index: implementations need not store term counts or positions.
// The returned frequency is the exact list length, used only to plan traversal.
// Missing keys return an empty iterator and zero frequency, without an error.
// The source must remain immutable and open until all iterators are closed.
// An implementation must release resources if opening fails.
type PostingIteratorIndex interface {
	OpenPostings(context.Context, string) (PostingIterator, uint64, error)
}

// IntersectTerms streams the AND of exact, already-analyzed keys in ascending
// ordinal order. It does not tokenize, rank, map IDs or hide tombstones.
// Candidates must be strictly increasing and use the same ordinal space as the
// index. Nil means unrestricted; a non-nil empty slice means no matches.
// An empty key list matches nothing. Repeated keys are ignored. Returning false
// from yield stops successfully; callers needing timestamp top-k must not stop
// prematurely.
// Neither candidates nor the index may be mutated during the call. Results
// yielded before an error are partial and must not be treated as complete.
func IntersectTerms(ctx context.Context, index PostingIteratorIndex, keys []string, candidates []DocOrd, yield func(DocOrd) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if index == nil || yield == nil {
		return fmt.Errorf("fts: nil iterator index or callback")
	}
	for i := 1; i < len(candidates); i++ {
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if candidates[i] <= candidates[i-1] {
			return fmt.Errorf("fts: candidates must be strictly increasing")
		}
	}
	var cursor PostingIterator
	if candidates != nil {
		cursor = &ordinalSliceIterator{ords: candidates, pos: -1}
	}
	return IntersectTermsWithIterator(ctx, index, keys, cursor, uint64(len(candidates)), yield)
}

// IntersectTermsWithIterator is IntersectTerms with a streaming candidate set,
// for example a bitmap cursor or an external-ID mapping. A nil cursor means
// unrestricted. Otherwise candidates must start before the first item and emit
// strictly increasing ordinals in the index's ordinal space. candidateCount is
// an upper bound on their number, used for planning; zero means empty.
// This function owns and closes candidates on every return, including errors.
// The candidate order is a precondition, not checked by materializing the stream.
func IntersectTermsWithIterator(ctx context.Context, index PostingIteratorIndex, keys []string, candidates PostingIterator, candidateCount uint64, yield func(DocOrd) bool) (err error) {
	type input struct {
		cursor PostingIterator
		count  uint64
	}
	inputs := make([]input, 0, len(keys)+1)
	if candidates != nil {
		inputs = append(inputs, input{cursor: candidates, count: candidateCount})
	}
	defer func() {
		for _, in := range inputs {
			if closeErr := in.cursor.Close(); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if index == nil || yield == nil {
		return fmt.Errorf("fts: nil iterator index or callback")
	}
	if len(keys) == 0 {
		return nil
	}
	if candidates != nil && candidateCount == 0 {
		return candidates.Err()
	}
	seen := make(map[string]bool, len(keys))
	for _, key := range keys {
		if seen[key] {
			continue
		}
		seen[key] = true
		if err := ctx.Err(); err != nil {
			return err
		}
		cursor, count, err := index.OpenPostings(ctx, key)
		if err != nil {
			return err
		}
		if cursor == nil {
			return fmt.Errorf("fts: nil postings iterator")
		}
		inputs = append(inputs, input{cursor: cursor, count: count})
		if count == 0 {
			return cursor.Err()
		}
	}
	slices.SortStableFunc(inputs, func(a, b input) int { return cmp.Compare(a.count, b.count) })
	driver := inputs[0].cursor
	for valid := driver.Next(); valid; {
		if err := ctx.Err(); err != nil {
			return err
		}
		target := driver.Ord()
		matched := true
		for _, in := range inputs[1:] {
			if !in.cursor.Advance(target) {
				return errors.Join(in.cursor.Err(), ctx.Err())
			}
			if in.cursor.Ord() > target {
				valid = driver.Advance(in.cursor.Ord())
				matched = false
				break
			}
		}
		if matched {
			if !yield(target) {
				return nil
			}
			valid = driver.Next()
		}
	}
	return errors.Join(driver.Err(), ctx.Err())
}

type ordinalSliceIterator struct {
	ords []DocOrd
	pos  int
}

func (i *ordinalSliceIterator) Next() bool {
	if i.pos < len(i.ords) {
		i.pos++
	}
	return i.pos < len(i.ords)
}

func (i *ordinalSliceIterator) Advance(target DocOrd) bool {
	if i.pos >= len(i.ords) {
		return false
	}
	if i.pos >= 0 && i.ords[i.pos] >= target {
		return true
	}
	start := i.pos + 1
	i.pos = start + sort.Search(len(i.ords)-start, func(n int) bool { return i.ords[start+n] >= target })
	return i.pos < len(i.ords)
}

func (i *ordinalSliceIterator) Ord() DocOrd { return i.ords[i.pos] }

func (i *ordinalSliceIterator) Err() error { return nil }

func (i *ordinalSliceIterator) Close() error {
	i.pos = len(i.ords)
	return nil
}
