package log

import (
	"io/ioutil"
	"os"
	"testing"

	api "github.com/felbit/golog/api/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scene, fun := range map[string]func(t *testing.T, log *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRange,
		"init with exisitng segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scene, func(t *testing.T) {
			dir, err := os.MkdirTemp(os.TempDir(), "store_test")
			assert.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			assert.NoError(t, err)

			fun(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("Hello, World!"),
	}
	off, err := log.Append(append)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	assert.NoError(t, err)
	assert.Equal(t, append.Value, read.Value)
}

func testOutOfRange(t *testing.T, log *Log) {
	read, err := log.Read(1)
	assert.Nil(t, read)
	assert.Error(t, err)
}

func testInitExisting(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("Hello, World!"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		assert.NoError(t, err)
	}
	assert.NoError(t, log.Close())

	off := log.LowestOffset()
	assert.Equal(t, uint64(0), off)
	off = log.HighestOffset()
	assert.Equal(t, uint64(2), off)

	new, err := NewLog(log.Dir, log.Config)
	assert.NoError(t, err)

	off = new.LowestOffset()
	assert.Equal(t, uint64(0), off)
	off = new.HighestOffset()
	assert.Equal(t, uint64(2), off)
}

func testReader(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("Hello, World!"),
	}
	off, err := log.Append(append)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	assert.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	assert.NoError(t, err)
	assert.Equal(t, append.Value, read.Value)
}

func testTruncate(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("Hello, World!"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		assert.NoError(t, err)
	}

	err := log.Truncate(1)
	assert.NoError(t, err)

	_, err = log.Read(0)
	assert.Error(t, err)
}
