package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/flat"
	"github.com/dariasmyr/fts-engine/pkg/index/presence"
	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

func main() {
	if err := run(os.Stdout); err != nil {
		panic(err)
	}
}

func run(out io.Writer) error {
	ctx := context.Background()
	// The embedding store owns numeric IDs, analyzer version and ordinal mapping.
	ids := []uint64{100000001, 100000008, 100000042}
	bodies := []string{"checkout timeout io.EOF", "billing timeout", "checkout timeout"}
	table, err := fts.NewOrdinalTable(ids)
	if err != nil {
		return err
	}
	// Use the same pipeline for documents and queries. Amber currently uses this
	// multilingual pipeline; ObservabilityPipeline has different semantics.
	pipe := textproc.DefaultMultilingualPipeline()
	builder := flat.New()
	for ord, body := range bodies {
		for _, key := range pipe.Process(body) {
			// Plain Insert avoids positional storage during build.
			if err := builder.Insert(key, fts.DocOrd(ord)); err != nil {
				return err
			}
		}
	}
	// Stream to a temporary file: no second complete encoded index on the heap.
	file, err := os.CreateTemp("", "fts-presence-*.fpr")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err := presence.WriteFromSource(ctx, file, builder); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	// A store would publish this file atomically together with the ID table and
	// analyzer metadata, then keep it open for the lifetime of its queries.
	reader, err := presence.OpenFile(file.Name())
	if err != nil {
		return err
	}
	defer reader.Close()
	// e.g. service=checkout from a bitmap index, in external ID space. The ID
	// absent from this segment is ignored. A non-nil empty filter matches nothing.
	candidates, count, err := table.Candidates(ctx, []uint64{100000001, 100000042, 100000099})
	if err != nil {
		return err
	}
	var outputErr error
	err = fts.IntersectTermsWithIterator(ctx, reader, pipe.Process("timeout"), candidates, count, func(ord fts.DocOrd) bool {
		id, ok := table.ID(ord)
		if !ok {
			outputErr = fmt.Errorf("ordinal %d is outside the ID table", ord)
			return false
		}
		_, outputErr = fmt.Fprintln(out, id)
		if outputErr != nil {
			return false
		}
		return true // Keep all candidates for subsequent predicates/time sorting.
	})
	if err != nil {
		return err
	}
	return outputErr
}
