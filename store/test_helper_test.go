package store

import (
	"os"
	"testing"
)

func setupTestStore(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}

	err = os.Chdir(dir)
	if err != nil {
		t.Fatalf("chdir err: %v", err)
	}
	t.Cleanup(func() {
		if err := Close(); err != nil {
			t.Errorf("close err: %v", err)
		}
		_ = os.Chdir(oldWd)
	})

	err = Open(DefaultConfig())
	if err != nil {
		t.Fatalf("open err: %v", err)
	}
}
