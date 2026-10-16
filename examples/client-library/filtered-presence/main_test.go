package main

import (
	"bytes"
	"testing"
)

func TestNumericIDsThroughFile(t *testing.T) {
	var out bytes.Buffer
	if err := run(&out); err != nil {
		t.Fatal(err)
	}
	if out.String() != "100000001\n100000042\n" {
		t.Fatal(out.String())
	}
}
