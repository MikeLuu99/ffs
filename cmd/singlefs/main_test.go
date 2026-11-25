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
	for _, test := range []struct {
		name string
		args []string
		want options
	}{
		{name: "defaults", want: options{data: "./data", mountpoint: "./mnt"}},
		{name: "custom mountpoint", args: []string{"./custom"}, want: options{data: "./data", mountpoint: "./custom"}},
		{name: "custom paths", args: []string{"-data", "./store", "./custom"}, want: options{data: "./store", mountpoint: "./custom"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseArgs(test.args, &output)
			if err != nil || got != test.want {
				t.Fatalf("parseArgs(%v) = %+v, %v; want %+v", test.args, got, err, test.want)
			}
		})
	}
	for _, args := range [][]string{{"one", "two"}, {"-unknown"}} {
		if _, err := parseArgs(args, &output); err == nil {
			t.Fatalf("invalid arguments accepted: %v", args)
		}
	}
}
