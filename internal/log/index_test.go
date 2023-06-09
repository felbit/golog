package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	assert.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	assert.NoError(t, err)

	_, _, err = idx.Read(-1)
	assert.Error(t, err)
	assert.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		assert.NoError(t, err)

		_, pos, err := idx.Read(int64(want.Off))
		assert.NoError(t, err)
		assert.Equal(t, want.Pos, pos)
	}

	// index and scanner should err when reading past existing entries
	_, _, err = idx.Read(int64(len(entries)))
	assert.ErrorIs(t, err, io.EOF)

	// index should build its state from existing file
	_ = idx.Close()
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0o600)
	idx, err = newIndex(f, c)
	assert.NoError(t, err)
	off, pos, err := idx.Read(-1)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), off)
	assert.Equal(t, entries[1].Pos, pos)
}
