package broker

import (
	"fmt"
	"testing"

	"github.com/aalyth/comet/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newTestBroker(t *testing.T) *Broker {
	t.Helper()

	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.EtcdEndpoints = nil // in-memory groups for tests

	b, err := New(cfg, zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })
	return b
}

func TestBrokerCreateTopic(t *testing.T) {
	b := newTestBroker(t)

	require.NoError(t, b.CreateTopic("events", 3))

	topics, err := b.ListTopics()
	require.NoError(t, err)
	require.Len(t, topics, 1)
	assert.Equal(t, "events", topics[0].Name)
	assert.Equal(t, int32(3), topics[0].Partitions)
}

func TestBrokerCreateTopicInvalidPartitions(t *testing.T) {
	b := newTestBroker(t)

	err := b.CreateTopic("bad", 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "positive")

	err = b.CreateTopic("bad", -1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "positive")
}

func TestBrokerDeleteTopic(t *testing.T) {
	b := newTestBroker(t)

	require.NoError(t, b.CreateTopic("temp", 1))
	require.NoError(t, b.DeleteTopic("temp"))

	topics, err := b.ListTopics()
	require.NoError(t, err)
	assert.Empty(t, topics)
}

func TestBrokerProduceAndConsume(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("data", 1))

	partition, offset, err := b.Produce("data", []byte("k1"), []byte("v1"))
	require.NoError(t, err)
	assert.Equal(t, int32(0), partition)
	assert.Equal(t, int64(0), offset)

	records, err := b.Consume("data", 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, int64(0), records[0].Offset)
	assert.Equal(t, []byte("k1"), records[0].Key)
	assert.Equal(t, []byte("v1"), records[0].Value)
}

func TestBrokerKeyPartitioning(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("keyed", 4))

	partitions := make(map[int32]bool)
	for i := 0; i < 10; i++ {
		p, _, err := b.Produce("keyed", []byte("same-key"), []byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
		partitions[p] = true
	}

	assert.Len(t, partitions, 1, "same key should always hash to same partition")
}

func TestBrokerRoundRobin(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("rr", 4))

	partitionCounts := make(map[int32]int)
	for i := 0; i < 8; i++ {
		p, _, err := b.Produce("rr", nil, []byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
		partitionCounts[p]++
	}

	assert.Len(t, partitionCounts, 4, "messages should hit all 4 partitions")
	for p, count := range partitionCounts {
		assert.Equal(t, 2, count, "partition %d should have exactly 2 messages", p)
	}
}

func TestBrokerProduceNonExistentTopic(t *testing.T) {
	b := newTestBroker(t)

	_, _, err := b.Produce("ghost", []byte("k"), []byte("v"))
	require.Error(t, err)
}

func TestBrokerConsumeNonExistentTopic(t *testing.T) {
	b := newTestBroker(t)

	_, err := b.Consume("ghost", 0, 0, 10)
	require.Error(t, err)
}

func TestBrokerConsumeInvalidPartition(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("small", 3))

	_, err := b.Consume("small", 99, 0, 10)
	require.Error(t, err)
}

func TestBrokerConsumeEmptyPartition(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("empty", 1))

	records, err := b.Consume("empty", 0, 0, 10)
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestBrokerMultipleTopics(t *testing.T) {
	b := newTestBroker(t)

	topics := []string{"t1", "t2", "t3"}
	for _, name := range topics {
		require.NoError(t, b.CreateTopic(name, 1))
	}

	for i, name := range topics {
		_, offset, err := b.Produce(name, []byte("k"), []byte(fmt.Sprintf("topic-%d", i)))
		require.NoError(t, err)
		assert.Equal(t, int64(0), offset, "each topic starts at offset 0")
	}

	for i, name := range topics {
		records, err := b.Consume(name, 0, 0, 10)
		require.NoError(t, err)
		require.Len(t, records, 1)
		assert.Equal(t, fmt.Sprintf("topic-%d", i), string(records[0].Value))
	}
}

func TestBrokerClose(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.EtcdEndpoints = nil

	b, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	require.NoError(t, b.CreateTopic("t1", 2))
	_, _, err = b.Produce("t1", []byte("k"), []byte("v"))
	require.NoError(t, err)

	assert.NoError(t, b.Close())
}
