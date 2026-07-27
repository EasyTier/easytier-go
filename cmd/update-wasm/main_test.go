package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestCoreArtifactPathUsesCargoTargetDirectory(t *testing.T) {
	source := filepath.Join(string(filepath.Separator), "checkout")
	for _, test := range []struct {
		name           string
		cargoTargetDir string
		wantTargetDir  string
	}{
		{name: "default", wantTargetDir: filepath.Join(source, "target")},
		{
			name:           "relative",
			cargoTargetDir: "build",
			wantTargetDir:  filepath.Join(source, "build"),
		},
		{
			name:           "absolute",
			cargoTargetDir: filepath.Join(string(filepath.Separator), "cache"),
			wantTargetDir:  filepath.Join(string(filepath.Separator), "cache"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			want := filepath.Join(test.wantTargetDir, coreArtifact)
			if got := coreArtifactPath(source, test.cargoTargetDir); got != want {
				t.Fatalf("core artifact path = %q, want %q", got, want)
			}
		})
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
