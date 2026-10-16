// Package presence provides immutable, unranked exact-term indexes for embedded
// stores that own their document IDs and filtering. It stores no frequencies,
// positions, analyzer metadata, tombstones or document registry. Use fts.IntersectTerms
// to intersect its postings with external candidate ordinals.
// The API and FPR1 format are experimental and separate from ranked FTSE segments.
package presence

const (
	magic          = "FPR1"
	footerSize     = 24
	blockSize      = 128
	blockEntrySize = 12 // last ordinal, relative offset, byte length
)
