package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestCommandArguments(t *testing.T) {
	var output bytes.Buffer
	if err := run([]string{"-h"}, &output); err != nil || !strings.Contains(output.String(), "Usage:") {
		t.Fatalf("help = %q, %v", output.String(), err)
	}
	for _, args := range [][]string{nil, {"one", "two"}, {"-unknown"}} {
		if err := run(args, &output); err == nil {
			t.Fatalf("invalid arguments accepted: %v", args)
		}
	}
}
