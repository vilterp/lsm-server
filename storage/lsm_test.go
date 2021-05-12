package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestLSM(t *testing.T) {
	dataDir := "../testdata"

	if err := os.RemoveAll(dataDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dataDir, 0700); err != nil {
		t.Fatal(err)
	}

	// writes
	writeLSM, err := NewLSM(dataDir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("foo%d", i)
		value := fmt.Sprintf("bar%d", i)
		if err := writeLSM.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}

	readLSM, err := NewLSM(dataDir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("foo%d", i)
		expectedValue := fmt.Sprintf("bar%d", i)

		value, found, err := readLSM.Get([]byte(key))
		if !found {
			t.Fatalf("key %s wasn't found", key)
		}
		if err != nil {
			t.Fatal(err)
		}
		if string(value) != expectedValue {
			t.Fatalf("for key %s: expected %s, got %s", key, expectedValue, value)
		}
	}
}
