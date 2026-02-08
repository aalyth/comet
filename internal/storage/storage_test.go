package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newTestStorage(t *testing.T) *Storage {
	t.Helper()
	dir := t.TempDir()
	s, err := New(dir, defaultTestSegmentMaxBytes, zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCreateTopic(t *testing.T) {
	s := newTestStorage(t)

	require.NoError(t, s.CreateTopic("orders", 3))

	topics := s.ListTopics()
	assert.Contains(t, topics, "orders")

	topic, err := s.GetTopic("orders")
	require.NoError(t, err)
	assert.Equal(t, int32(3), topic.PartitionCount())
}

func TestCreateDuplicateTopic(t *testing.T) {
	s := newTestStorage(t)

	require.NoError(t, s.CreateTopic("dup", 1))
	err := s.CreateTopic("dup", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestDeleteTopic(t *testing.T) {
	s := newTestStorage(t)

	require.NoError(t, s.CreateTopic("ephemeral", 2))
	require.NoError(t, s.DeleteTopic("ephemeral"))

	topics := s.ListTopics()
	assert.NotContains(t, topics, "ephemeral")

	topicDir := filepath.Join(s.dataDir, "ephemeral")
	_, err := os.Stat(topicDir)
	assert.True(t, os.IsNotExist(err), "topic directory should be removed from disk")
}

func TestDeleteNonExistent(t *testing.T) {
	s := newTestStorage(t)

	err := s.DeleteTopic("ghost")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestGetTopicNotFound(t *testing.T) {
	s := newTestStorage(t)

	_, err := s.GetTopic("missing")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestListTopicsEmpty(t *testing.T) {
	s := newTestStorage(t)

	topics := s.ListTopics()
	assert.Empty(t, topics)
}

func TestListTopicsMultiple(t *testing.T) {
	s := newTestStorage(t)

	names := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, name := range names {
		require.NoError(t, s.CreateTopic(name, 1))
	}

	topics := s.ListTopics()
	sort.Strings(topics)
	sort.Strings(names)
	assert.Equal(t, names, topics)
}

func TestGetPartition(t *testing.T) {
	s := newTestStorage(t)
	require.NoError(t, s.CreateTopic("parts", 3))

	topic, err := s.GetTopic("parts")
	require.NoError(t, err)

	for i := int32(0); i < 3; i++ {
		p, err := topic.GetPartition(i)
		assert.NoError(t, err)
		assert.NotNil(t, p)
	}

	_, err = topic.GetPartition(999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestStorageClose(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir, defaultTestSegmentMaxBytes, zap.NewNop())
	require.NoError(t, err)

	require.NoError(t, s.CreateTopic("t1", 2))
	require.NoError(t, s.CreateTopic("t2", 1))
	assert.NoError(t, s.Close())
}

func TestTopicDirectoryStructure(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir, defaultTestSegmentMaxBytes, zap.NewNop())
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.CreateTopic("orders", 3))

	for i := 0; i < 3; i++ {
		partDir := filepath.Join(dir, "orders", fmt.Sprintf("%d", i))
		info, err := os.Stat(partDir)
		require.NoError(t, err, "partition dir %d should exist", i)
		assert.True(t, info.IsDir())

		// each partition dir should contain the initial segment file
		segFile := filepath.Join(partDir, "00000000000000000000.log")
		_, err = os.Stat(segFile)
		assert.NoError(t, err, "initial segment file should exist in partition %d", i)
	}
}
