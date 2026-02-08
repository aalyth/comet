package broker

import (
	"fmt"
	"testing"

	"github.com/aalyth/comet/internal/config"
	pb "github.com/aalyth/comet/proto/comet/v1"
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

	require.NoError(t, b.CreateTopic("events", 3, 1))

	topics, err := b.ListTopics()
	require.NoError(t, err)
	require.Len(t, topics, 1)
	assert.Equal(t, "events", topics[0].Name)
	assert.Equal(t, int32(3), topics[0].Partitions)
	assert.Equal(t, int32(1), topics[0].ReplicationFactor)
}

func TestBrokerCreateTopicInvalidPartitions(t *testing.T) {
	b := newTestBroker(t)

	err := b.CreateTopic("bad", 0, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "positive")

	err = b.CreateTopic("bad", -1, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "positive")
}

func TestBrokerCreateTopicInvalidReplicationFactor(t *testing.T) {
	b := newTestBroker(t)

	err := b.CreateTopic("bad", 3, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "positive")

	// replication factor exceeds broker count (1 broker)
	err = b.CreateTopic("bad", 3, 5)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds")
}

func TestBrokerDeleteTopic(t *testing.T) {
	b := newTestBroker(t)

	require.NoError(t, b.CreateTopic("temp", 1, 1))
	require.NoError(t, b.DeleteTopic("temp"))

	topics, err := b.ListTopics()
	require.NoError(t, err)
	assert.Empty(t, topics)
}

func TestBrokerProduceAndConsume(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("data", 1, 1))

	results, err := b.Produce(
		"data", []*pb.ProduceRecord{
			{Partition: 0, Key: []byte("k1"), Value: []byte("v1")},
		},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int32(0), results[0].Partition)
	assert.Equal(t, int64(0), results[0].Offset)

	records, err := b.Consume("data", 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, int64(0), records[0].Offset)
	assert.Equal(t, []byte("k1"), records[0].Key)
	assert.Equal(t, []byte("v1"), records[0].Value)
}

func TestBrokerBatchProduce(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("batch", 1, 1))

	records := make([]*pb.ProduceRecord, 5)
	for i := 0; i < 5; i++ {
		records[i] = &pb.ProduceRecord{
			Partition: 0,
			Key:       []byte(fmt.Sprintf("k%d", i)),
			Value:     []byte(fmt.Sprintf("v%d", i)),
		}
	}

	results, err := b.Produce("batch", records)
	require.NoError(t, err)
	require.Len(t, results, 5)

	for i, r := range results {
		assert.Equal(t, int64(i), r.Offset)
		assert.Equal(t, int32(0), r.Partition)
	}

	consumed, err := b.Consume("batch", 0, 0, 10)
	require.NoError(t, err)
	assert.Len(t, consumed, 5)
}

func TestBrokerAutoSelectPartition(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("auto", 4, 1))

	// negative partition triggers auto-selection
	records := make([]*pb.ProduceRecord, 8)
	for i := 0; i < 8; i++ {
		records[i] = &pb.ProduceRecord{
			Partition: -1,
			Value:     []byte(fmt.Sprintf("v%d", i)),
		}
	}

	results, err := b.Produce("auto", records)
	require.NoError(t, err)
	require.Len(t, results, 8)

	partitions := make(map[int32]bool)
	for _, r := range results {
		partitions[r.Partition] = true
	}
	assert.Greater(t, len(partitions), 1, "messages should hit multiple partitions")
}

func TestBrokerKeyPartitioning(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("keyed", 4, 1))

	partitions := make(map[int32]bool)
	for i := 0; i < 10; i++ {
		results, err := b.Produce(
			"keyed", []*pb.ProduceRecord{
				{Partition: -1, Key: []byte("same-key"), Value: []byte(fmt.Sprintf(
					"v%d", i,
				))},
			},
		)
		require.NoError(t, err)
		partitions[results[0].Partition] = true
	}

	assert.Len(t, partitions, 1, "same key should always hash to same partition")
}

func TestBrokerProduceNonExistentTopic(t *testing.T) {
	b := newTestBroker(t)

	_, err := b.Produce(
		"ghost", []*pb.ProduceRecord{
			{Partition: 0, Key: []byte("k"), Value: []byte("v")},
		},
	)
	require.Error(t, err)
}

func TestBrokerConsumeNonExistentTopic(t *testing.T) {
	b := newTestBroker(t)

	_, err := b.Consume("ghost", 0, 0, 10)
	require.Error(t, err)
}

func TestBrokerConsumeInvalidPartition(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("small", 3, 1))

	_, err := b.Consume("small", 99, 0, 10)
	require.Error(t, err)
}

func TestBrokerConsumeEmptyPartition(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("empty", 1, 1))

	records, err := b.Consume("empty", 0, 0, 10)
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestBrokerMultipleTopics(t *testing.T) {
	b := newTestBroker(t)

	topics := []string{"t1", "t2", "t3"}
	for _, name := range topics {
		require.NoError(t, b.CreateTopic(name, 1, 1))
	}

	for i, name := range topics {
		results, err := b.Produce(
			name, []*pb.ProduceRecord{
				{Partition: 0, Key: []byte("k"), Value: []byte(fmt.Sprintf(
					"topic-%d", i,
				))},
			},
		)
		require.NoError(t, err)
		assert.Equal(t, int64(0), results[0].Offset, "each topic starts at offset 0")
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

	require.NoError(t, b.CreateTopic("t1", 2, 1))
	_, err = b.Produce(
		"t1", []*pb.ProduceRecord{
			{Partition: 0, Key: []byte("k"), Value: []byte("v")},
		},
	)
	require.NoError(t, err)

	assert.NoError(t, b.Close())
}

func TestBrokerNotLeader(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("leader-test", 2, 1))

	// manually set a different leader for partition 0
	assignment, ok := b.assignments.GetAssignment("leader-test")
	require.True(t, ok)

	// change partition 0 leader to a non-existent broker
	assignment.Partitions[0].LeaderID = "other-broker"
	require.NoError(t, b.assignments.SaveAssignment("leader-test", *assignment))

	_, err := b.Produce(
		"leader-test", []*pb.ProduceRecord{
			{Partition: 0, Key: []byte("k"), Value: []byte("v")},
		},
	)
	require.Error(t, err)

	var notLeader *ErrNotLeader
	assert.ErrorAs(t, err, &notLeader)
	assert.Equal(t, "other-broker", notLeader.LeaderID)
}

// --- Consumer Group

func TestBrokerJoinGroup(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("grp", 3, 1))

	member, gen, err := b.JoinGroup("grp", "group-1")
	require.NoError(t, err)
	assert.NotEmpty(t, member.ID)
	assert.Equal(t, int64(1), gen)
	assert.Len(t, member.Partitions(), 3, "sole member should own all partitions")
}

func TestBrokerJoinGroupMultipleMembers(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("grp", 4, 1))

	m1, _, err := b.JoinGroup("grp", "group-1")
	require.NoError(t, err)

	m2, gen2, err := b.JoinGroup("grp", "group-1")
	require.NoError(t, err)

	assert.Equal(t, int64(2), gen2, "generation should increment on rebalance")

	// together they should cover all 4 partitions
	allPartitions := make(map[int32]bool)
	for _, p := range m1.Partitions() {
		allPartitions[p] = true
	}
	for _, p := range m2.Partitions() {
		allPartitions[p] = true
	}
	assert.Len(t, allPartitions, 4)
}

func TestBrokerLeaveGroup(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("grp", 2, 1))

	m1, _, err := b.JoinGroup("grp", "group-1")
	require.NoError(t, err)

	m2, _, err := b.JoinGroup("grp", "group-1")
	require.NoError(t, err)

	require.NoError(t, b.LeaveGroup("grp", "group-1", m1.ID))

	// m2 should now own all partitions
	_, _, gen, err := b.PollGroup("grp", "group-1", m2.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(3), gen)
}

func TestBrokerPollGroup(t *testing.T) {
	b := newTestBroker(t)
	require.NoError(t, b.CreateTopic("grp", 2, 1))

	member, _, err := b.JoinGroup("grp", "group-1")
	require.NoError(t, err)

	partitions, rebalance, gen, err := b.PollGroup("grp", "group-1", member.ID)
	require.NoError(t, err)
	assert.False(t, rebalance)
	assert.Equal(t, int64(1), gen)
	assert.Len(t, partitions, 2)
}
