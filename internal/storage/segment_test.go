package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/aalyth/comet/proto/comet/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func makeRecord(offset int64, key, value string) *pb.WalRecord {
	return &pb.WalRecord{
		Offset:    offset,
		Key:       []byte(key),
		Value:     []byte(value),
		Timestamp: time.Now().UnixNano(),
	}
}

func TestNewWalSegment(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewWalSegment(dir, 0, zap.NewNop())
	require.NoError(t, err)
	defer seg.Close()

	expectedPath := filepath.Join(dir, "00000000000000000000.log")
	_, err = os.Stat(expectedPath)
	assert.NoError(t, err, "segment file should exist")
	assert.Equal(t, int64(0), seg.Size(), "initial size should be 0")
	assert.Equal(t, int64(0), seg.baseOffset)
}

func TestSegmentAppendAndRead(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewWalSegment(dir, 0, zap.NewNop())
	require.NoError(t, err)
	defer seg.Close()

	record := makeRecord(0, "key-0", "value-0")
	require.NoError(t, seg.Append(record))

	got, err := seg.ReadAt(0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), got.Offset)
	assert.Equal(t, []byte("key-0"), got.Key)
	assert.Equal(t, []byte("value-0"), got.Value)
	assert.Equal(t, record.Timestamp, got.Timestamp)
}

func TestSegmentMultipleRecords(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewWalSegment(dir, 0, zap.NewNop())
	require.NoError(t, err)
	defer seg.Close()

	const count = 100
	for i := int64(0); i < count; i++ {
		record := makeRecord(i, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
		require.NoError(t, seg.Append(record))
	}

	for i := int64(0); i < count; i++ {
		got, err := seg.ReadAt(i)
		require.NoError(t, err)
		assert.Equal(t, i, got.Offset)
		assert.Equal(t, fmt.Sprintf("key-%d", i), string(got.Key))
		assert.Equal(t, fmt.Sprintf("value-%d", i), string(got.Value))
	}
}

func TestSegmentReadNonExistent(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewWalSegment(dir, 0, zap.NewNop())
	require.NoError(t, err)
	defer seg.Close()

	record := makeRecord(0, "key", "value")
	require.NoError(t, seg.Append(record))

	_, err = seg.ReadAt(999)
	assert.ErrorIs(t, err, ErrRecordNotFound)
}

func TestSegmentBuildIndex(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewWalSegment(dir, 0, zap.NewNop())
	require.NoError(t, err)

	const count = 20
	for i := int64(0); i < count; i++ {
		record := makeRecord(i, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
		require.NoError(t, seg.Append(record))
	}
	require.NoError(t, seg.Close())

	// reopen the same segment file — index should be rebuilt from disk
	seg2, err := NewWalSegment(dir, 0, zap.NewNop())
	require.NoError(t, err)
	defer seg2.Close()

	for i := int64(0); i < count; i++ {
		got, err := seg2.ReadAt(i)
		require.NoError(t, err, "offset %d should be readable after reopen", i)
		assert.Equal(t, i, got.Offset)
		assert.Equal(t, fmt.Sprintf("k%d", i), string(got.Key))
		assert.Equal(t, fmt.Sprintf("v%d", i), string(got.Value))
	}
}

func TestSegmentSizeTracking(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewWalSegment(dir, 0, zap.NewNop())
	require.NoError(t, err)
	defer seg.Close()

	prevSize := seg.Size()
	assert.Equal(t, int64(0), prevSize)

	for i := int64(0); i < 10; i++ {
		record := makeRecord(i, "key", "value")
		require.NoError(t, seg.Append(record))

		newSize := seg.Size()
		assert.Greater(t, newSize, prevSize, "size should grow after append")
		prevSize = newSize
	}
}

func TestSegmentConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewWalSegment(dir, 0, zap.NewNop())
	require.NoError(t, err)
	defer seg.Close()

	// pre-populate some records so readers have something to read
	for i := int64(0); i < 50; i++ {
		require.NoError(t, seg.Append(makeRecord(i, "key", "value")))
	}

	var wg sync.WaitGroup
	const writers = 5
	const readers = 5
	const opsPerGoroutine = 20

	// writers append offsets [50; 149]
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				offset := int64(50 + writerID*opsPerGoroutine + j)
				err := seg.Append(makeRecord(offset, "key", "value"))
				assert.NoError(t, err)
			}
		}(w)
	}

	// readers read from pre-populated range [0; 49]
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				offset := int64(j % 50)
				_, err := seg.ReadAt(offset)
				assert.NoError(t, err)
			}
		}()
	}

	wg.Wait()
}

func TestSegmentBaseOffset(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewWalSegment(dir, 500, zap.NewNop())
	require.NoError(t, err)
	defer seg.Close()

	expectedPath := filepath.Join(dir, "00000000000000000500.log")
	_, err = os.Stat(expectedPath)
	assert.NoError(t, err, "segment file should use the base offset in its name")
	assert.Equal(t, int64(500), seg.baseOffset)
}
