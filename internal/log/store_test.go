package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	write = []byte("Hello, world!")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "store_append_read_test")
	assert.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	assert.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	assert.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		assert.NoError(t, err)
		assert.Equal(t, width*i, pos+n)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		assert.NoError(t, err)
		assert.Equal(t, write, read)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		assert.NoError(t, err)
		assert.Equal(t, lenWidth, n)
		off += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		assert.NoError(t, err)
		assert.Equal(t, int(size), n)
		assert.Equal(t, write, b)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	assert.NoError(t, err)
	defer os.Remove(f.Name())
	s, err := newStore(f)
	assert.NoError(t, err)
	_, _, err = s.Append(write)
	assert.NoError(t, err)
	f, before, err := openFile(f.Name())
	assert.NoError(t, err)
	err = s.Close()
	assert.NoError(t, err)
	_, after, err := openFile(f.Name())
	assert.NoError(t, err)
	assert.True(t, after > before)
}

func openFile(name string) (*os.File, int64, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
