package broker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeAssignmentsBasic(t *testing.T) {
	brokers := []string{"b1", "b2", "b3"}
	assignments, err := computeAssignments(brokers, 6, 1)
	require.NoError(t, err)

	assert.Len(t, assignments, 6)

	assert.Equal(t, "b1", assignments[0].LeaderID)
	assert.Equal(t, "b2", assignments[1].LeaderID)
	assert.Equal(t, "b3", assignments[2].LeaderID)
	assert.Equal(t, "b1", assignments[3].LeaderID)
	assert.Equal(t, "b2", assignments[4].LeaderID)
	assert.Equal(t, "b3", assignments[5].LeaderID)
}

func TestComputeAssignmentsWithReplication(t *testing.T) {
	brokers := []string{"b1", "b2", "b3"}
	assignments, err := computeAssignments(brokers, 3, 2)
	require.NoError(t, err)

	// p0: leader=b1, replicas=[b1, b2]
	assert.Equal(t, "b1", assignments[0].LeaderID)
	assert.Equal(t, []string{"b1", "b2"}, assignments[0].ReplicaIDs)

	// p1: leader=b2, replicas=[b2, b3]
	assert.Equal(t, "b2", assignments[1].LeaderID)
	assert.Equal(t, []string{"b2", "b3"}, assignments[1].ReplicaIDs)

	// p2: leader=b3, replicas=[b3, b1]
	assert.Equal(t, "b3", assignments[2].LeaderID)
	assert.Equal(t, []string{"b3", "b1"}, assignments[2].ReplicaIDs)
}

func TestComputeAssignmentsFullReplication(t *testing.T) {
	brokers := []string{"b1", "b2", "b3"}
	assignments, err := computeAssignments(brokers, 2, 3)
	require.NoError(t, err)

	// every partition should be replicated to all 3 brokers
	for _, a := range assignments {
		assert.Len(t, a.ReplicaIDs, 3)
	}
}

func TestComputeAssignmentsNoBrokers(t *testing.T) {
	_, err := computeAssignments(nil, 3, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no brokers")
}

func TestComputeAssignmentsReplicationExceedsBrokers(t *testing.T) {
	brokers := []string{"b1", "b2"}
	_, err := computeAssignments(brokers, 3, 3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds")
}

func TestComputeAssignmentsInvalidReplicationFactor(t *testing.T) {
	brokers := []string{"b1"}
	_, err := computeAssignments(brokers, 1, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "positive")
}

func TestComputeAssignmentsDeterministic(t *testing.T) {
	brokers := []string{"b3", "b1", "b2"} // intentionally unordered

	a1, err := computeAssignments(brokers, 4, 2)
	require.NoError(t, err)

	a2, err := computeAssignments(brokers, 4, 2)
	require.NoError(t, err)

	for p := int32(0); p < 4; p++ {
		assert.Equal(
			t, a1[p].LeaderID, a2[p].LeaderID,
			"partition %d leader should be deterministic", p,
		)
		assert.Equal(
			t, a1[p].ReplicaIDs, a2[p].ReplicaIDs,
			"partition %d replicas should be deterministic", p,
		)
	}
}

func TestComputeAssignmentsSingleBroker(t *testing.T) {
	brokers := []string{"solo"}
	assignments, err := computeAssignments(brokers, 5, 1)
	require.NoError(t, err)

	for _, a := range assignments {
		assert.Equal(t, "solo", a.LeaderID)
		assert.Equal(t, []string{"solo"}, a.ReplicaIDs)
	}
}

func TestAssignmentManagerInMemory(t *testing.T) {
	am := NewAssignmentManager(nil, nil)
	defer am.Close()

	meta := TopicMeta{Partitions: 3, ReplicationFactor: 1}
	require.NoError(t, am.SaveTopicMeta("test", meta))

	got, ok := am.GetTopicMeta("test")
	require.True(t, ok)
	assert.Equal(t, int32(3), got.Partitions)
	assert.Equal(t, int32(1), got.ReplicationFactor)

	assignment := TopicAssignment{
		TopicName:         "test",
		NumPartitions:     3,
		ReplicationFactor: 1,
		Partitions: map[int32]*PartitionAssignment{
			0: {LeaderID: "b1", ReplicaIDs: []string{"b1"}},
			1: {LeaderID: "b1", ReplicaIDs: []string{"b1"}},
			2: {LeaderID: "b1", ReplicaIDs: []string{"b1"}},
		},
	}
	require.NoError(t, am.SaveAssignment("test", assignment))

	gotAssignment, ok := am.GetAssignment("test")
	require.True(t, ok)
	assert.Equal(t, "test", gotAssignment.TopicName)
	assert.Len(t, gotAssignment.Partitions, 3)

	require.NoError(t, am.DeleteTopicMeta("test"))
	_, ok = am.GetTopicMeta("test")
	assert.False(t, ok)
	_, ok = am.GetAssignment("test")
	assert.False(t, ok)
}

func TestReassignAll(t *testing.T) {
	am := NewAssignmentManager(nil, nil)
	defer am.Close()

	require.NoError(t, am.SaveTopicMeta("t1", TopicMeta{Partitions: 4, ReplicationFactor: 2}))
	require.NoError(t, am.SaveTopicMeta("t2", TopicMeta{Partitions: 2, ReplicationFactor: 1}))

	am.ReassignAll([]string{"b1", "b2", "b3"})

	a1, ok := am.GetAssignment("t1")
	require.True(t, ok)
	assert.Len(t, a1.Partitions, 4)

	// each partition of t1 should have 2 replicas
	for _, pa := range a1.Partitions {
		assert.Len(t, pa.ReplicaIDs, 2)
	}

	a2, ok := am.GetAssignment("t2")
	require.True(t, ok)
	assert.Len(t, a2.Partitions, 2)
}
