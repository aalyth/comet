package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const defaultTestSegmentMaxBytes int64 = 1024 * 1024 * 1024

func TestNewPartition(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, 0, defaultTestSegmentMaxBytes, zap.NewNop())
	require.NoError(t, err)
	defer p.Close()

	assert.Len(t, p.segments, 1)
	assert.Equal(t, int64(0), p.nextOffset)

	expectedFile := filepath.Join(dir, "00000000000000000000.log")
	_, err = os.Stat(expectedFile)
	assert.NoError(t, err, "initial segment file should exist")
}

func TestPartitionAppendAndReadAt(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, 0, defaultTestSegmentMaxBytes, zap.NewNop())
	require.NoError(t, err)
	defer p.Close()

	offset, err := p.Append([]byte("key-0"), []byte("value-0"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), offset)

	record, err := p.ReadAt(0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), record.Offset)
	assert.Equal(t, []byte("key-0"), record.Key)
	assert.Equal(t, []byte("value-0"), record.Value)
	assert.Greater(t, record.Timestamp, int64(0))
}

func TestPartitionSequentialOffsets(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, 0, defaultTestSegmentMaxBytes, zap.NewNop())
	require.NoError(t, err)
	defer p.Close()

	const count = 50
	for i := int64(0); i < count; i++ {
		offset, err := p.Append([]byte("k"), []byte("v"))
		require.NoError(t, err)
		assert.Equal(t, i, offset, "offsets should be sequential")
	}
}

func TestPartitionReadFrom(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, 0, defaultTestSegmentMaxBytes, zap.NewNop())
	require.NoError(t, err)
	defer p.Close()

	for i := 0; i < 20; i++ {
		_, err := p.Append([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}

	records, err := p.ReadFrom(5, 10)
	require.NoError(t, err)
	require.Len(t, records, 10)

	for i, rec := range records {
		expected := int64(5 + i)
		assert.Equal(t, expected, rec.Offset)
		assert.Equal(t, fmt.Sprintf("k%d", expected), string(rec.Key))
		assert.Equal(t, fmt.Sprintf("v%d", expected), string(rec.Value))
	}
}

func TestPartitionReadFromBeyondEnd(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, 0, defaultTestSegmentMaxBytes, zap.NewNop())
	require.NoError(t, err)
	defer p.Close()

	for i := 0; i < 5; i++ {
		_, err := p.Append([]byte("k"), []byte("v"))
		require.NoError(t, err)
	}

	records, err := p.ReadFrom(5, 10)
	require.NoError(t, err)
	assert.Empty(t, records, "reading beyond nextOffset should return empty")
}

func TestPartitionReadAtInvalid(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, 0, defaultTestSegmentMaxBytes, zap.NewNop())
	require.NoError(t, err)
	defer p.Close()

	_, err = p.ReadAt(0)
	assert.ErrorIs(t, err, ErrRecordNotFound)

	_, err = p.Append([]byte("k"), []byte("v"))
	require.NoError(t, err)

	_, err = p.ReadAt(999)
	assert.ErrorIs(t, err, ErrRecordNotFound)
}

func TestPartitionReadFromMiddle(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, 0, defaultTestSegmentMaxBytes, zap.NewNop())
	require.NoError(t, err)
	defer p.Close()

	for i := 0; i < 10; i++ {
		_, err := p.Append([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}

	records, err := p.ReadFrom(7, 100)
	require.NoError(t, err)
	require.Len(t, records, 3)

	for i, rec := range records {
		assert.Equal(t, int64(7+i), rec.Offset)
	}
}

func TestPartitionConcurrentAppend(t *testing.T) {
	dir := t.TempDir()

	p, err := NewPartition(dir, 0, defaultTestSegmentMaxBytes, zap.NewNop())
	require.NoError(t, err)
	defer p.Close()

	const goroutines = 10
	const perGoroutine = 50

	var mu sync.Mutex
	offsets := make(map[int64]bool)
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				offset, err := p.Append([]byte("k"), []byte("v"))
				assert.NoError(t, err)

				mu.Lock()
				offsets[offset] = true
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	expectedTotal := goroutines * perGoroutine
	assert.Len(t, offsets, expectedTotal, "all offsets should be unique")

	records, err := p.ReadFrom(0, expectedTotal)
	require.NoError(t, err)
	assert.Len(t, records, expectedTotal)
}

func TestPartitionSegmentRotation(t *testing.T) {
	dir := t.TempDir()

	const tinyMaxSegmentSize int64 = 256
	p, err := NewPartition(dir, 0, tinyMaxSegmentSize, zap.NewNop())
	require.NoError(t, err)
	defer p.Close()

	// write enough records to exceed tinyMaxSegmentSize multiple times
	const count = 100
	for i := 0; i < count; i++ {
		_, err := p.Append(
			[]byte(fmt.Sprintf("key-%04d", i)),
			[]byte(fmt.Sprintf("value-%04d", i)),
		)
		require.NoError(t, err)
	}

	assert.Greater(t, len(p.segments), 1, "should have rotated into multiple segments")

	// verify multiple segment files on disk
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var logFiles []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			logFiles = append(logFiles, e.Name())
		}
	}
	sort.Strings(logFiles)

	assert.Greater(t, len(logFiles), 1, "should have multiple .log files on disk")
	assert.Equal(t, "00000000000000000000.log", logFiles[0], "first segment starts at offset 0")

	records, err := p.ReadFrom(0, count)
	require.NoError(t, err)
	require.Len(t, records, count)

	for i, rec := range records {
		assert.Equal(t, int64(i), rec.Offset)
		assert.Equal(t, fmt.Sprintf("key-%04d", i), string(rec.Key))
		assert.Equal(t, fmt.Sprintf("value-%04d", i), string(rec.Value))
	}
}

func TestPartitionSegmentRotationReadAcross(t *testing.T) {
	dir := t.TempDir()

	const tinyMax int64 = 128
	p, err := NewPartition(dir, 0, tinyMax, zap.NewNop())
	require.NoError(t, err)
	defer p.Close()

	const count = 50
	for i := 0; i < count; i++ {
		_, err := p.Append([]byte("k"), []byte(fmt.Sprintf("val-%d", i)))
		require.NoError(t, err)
	}

	require.Greater(t, len(p.segments), 1, "need multiple segments for this test")

	// read across segment boundaries starting from the middle
	midpoint := int64(count / 2)
	records, err := p.ReadFrom(midpoint, count)
	require.NoError(t, err)

	expected := count - int(midpoint)
	require.Len(t, records, expected)

	for i, rec := range records {
		assert.Equal(t, midpoint+int64(i), rec.Offset)
	}
}
