package storage

import (
	"fmt"
	"testing"
)

func TestLSM(t *testing.T) {
	lsm, err := NewLSM("../testdata")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("foo%d", i)
		if err := lsm.Put([]byte(key), []byte("bar")); err != nil {
			t.Fatal(err)
		}
	}
}
