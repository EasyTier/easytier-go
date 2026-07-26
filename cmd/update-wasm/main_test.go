package main

import (
	"strings"
	"testing"
)

func TestCoreFeaturesIncludeAcceleratedEncryption(t *testing.T) {
	features := "," + coreFeatures + ","
	if !strings.Contains(features, ",ring-crypto,") {
		t.Fatalf("WASM features do not include accelerated encryption: %s", coreFeatures)
	}
}

func TestReproducibleBuildEnvironmentControlsRustInputs(t *testing.T) {
	t.Setenv("RUSTFLAGS", "-C target-cpu=native")
	t.Setenv("CARGO_ENCODED_RUSTFLAGS", "-C\u001fopt-level=1")
	t.Setenv("SOURCE_DATE_EPOCH", "1")
	environment := reproducibleBuildEnvironment("/checkout", "1700000000")
	joined := strings.Join(environment, "\n")
	if strings.Contains(joined, "target-cpu=native") ||
		strings.Contains(joined, "opt-level=1") {
		t.Fatalf("uncontrolled Rust flags remained in environment:\n%s", joined)
	}
	for _, want := range []string{
		"CARGO_ENCODED_RUSTFLAGS=--remap-path-prefix=/checkout=/workspace/EasyTier",
		"SOURCE_DATE_EPOCH=1700000000",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("build environment does not contain %q", want)
		}
	}
}
