package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	assignmentKeyPrefix = "/comet/assignments/"
	topicMetaKeyPrefix  = "/comet/meta/"
)

type PartitionAssignment struct {
	LeaderID   string   `json:"leader_id"`
	ReplicaIDs []string `json:"replica_ids"`
}

type TopicAssignment struct {
	TopicName         string                         `json:"topic_name"`
	Partitions        map[int32]*PartitionAssignment `json:"partitions"`
	ReplicationFactor int32                          `json:"replication_factor"`
	NumPartitions     int32                          `json:"num_partitions"`
}

type TopicMeta struct {
	Partitions        int32 `json:"partitions"`
	ReplicationFactor int32 `json:"replication_factor"`
}

func computeAssignments(
	brokerIDs []string,
	numPartitions, replicationFactor int32,
) (map[int32]*PartitionAssignment, error) {
	n := int32(len(brokerIDs))
	if n == 0 {
		return nil, fmt.Errorf("no brokers available")
	}

	if replicationFactor > n {
		return nil, fmt.Errorf(
			"replication factor %d exceeds broker count %d",
			replicationFactor, n,
		)
	}
	if replicationFactor <= 0 {
		return nil, fmt.Errorf("replication factor must be positive")
	}

	sorted := make([]string, len(brokerIDs))
	copy(sorted, brokerIDs)
	sort.Strings(sorted)

	assignments := make(map[int32]*PartitionAssignment, numPartitions)
	for p := int32(0); p < numPartitions; p++ {
		leaderIdx := int(p) % int(n)
		leader := sorted[leaderIdx]

		replicas := make([]string, 0, replicationFactor)
		replicas = append(replicas, leader)
		for r := int32(1); r < replicationFactor; r++ {
			replicaIdx := (leaderIdx + int(r)) % int(n)
			replicas = append(replicas, sorted[replicaIdx])
		}

		assignments[p] = &PartitionAssignment{
			LeaderID:   leader,
			ReplicaIDs: replicas,
		}
	}
	return assignments, nil
}

// AssignmentManager watches /comet/assignments/ and /comet/meta/ in etcd and
// maintains an in-memory cache of all topic assignments.
type AssignmentManager struct {
	etcd *clientv3.Client

	mu     sync.RWMutex
	topics map[string]*TopicAssignment
	meta   map[string]*TopicMeta
	notify chan struct{}

	logger *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewAssignmentManager(etcd *clientv3.Client, logger *zap.Logger) *AssignmentManager {
	ctx, cancel := context.WithCancel(context.Background())
	if logger == nil {
		logger = zap.NewNop()
	}
	am := &AssignmentManager{
		etcd:   etcd,
		topics: make(map[string]*TopicAssignment),
		meta:   make(map[string]*TopicMeta),
		notify: make(chan struct{}),
		logger: logger.Named("assignments"),
		ctx:    ctx,
		cancel: cancel,
	}

	if etcd != nil {
		am.syncFromEtcd()
		am.wg.Add(1)
		go am.watchAssignments()
	}

	return am
}

func (am *AssignmentManager) SaveTopicMeta(topic string, meta TopicMeta) error {
	am.mu.Lock()
	am.meta[topic] = &meta
	am.mu.Unlock()

	if am.etcd == nil {
		return nil
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Wrap(err, "marshalling topic meta")
	}

	key := topicMetaKeyPrefix + topic
	_, err = am.etcd.Put(am.ctx, key, string(data))
	return errors.Wrap(err, "saving topic meta to etcd")
}

func (am *AssignmentManager) DeleteTopicMeta(topic string) error {
	am.mu.Lock()
	delete(am.meta, topic)
	delete(am.topics, topic)
	am.mu.Unlock()

	if am.etcd == nil {
		return nil
	}

	_, err := am.etcd.Delete(am.ctx, topicMetaKeyPrefix+topic)
	if err != nil {
		return errors.Wrap(err, "deleting topic meta from etcd")
	}

	_, err = am.etcd.Delete(am.ctx, assignmentKeyPrefix+topic)
	return errors.Wrap(err, "deleting assignment from etcd")
}

func (am *AssignmentManager) GetTopicMeta(topic string) (*TopicMeta, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	m, ok := am.meta[topic]
	return m, ok
}

func (am *AssignmentManager) AllTopicsMeta() map[string]*TopicMeta {
	am.mu.RLock()
	defer am.mu.RUnlock()
	cp := make(map[string]*TopicMeta, len(am.meta))
	for k, v := range am.meta {
		cp[k] = v
	}
	return cp
}

func (am *AssignmentManager) SaveAssignment(topic string, assignment TopicAssignment) error {
	am.mu.Lock()
	am.topics[topic] = &assignment
	am.mu.Unlock()

	if am.etcd == nil {
		return nil
	}

	data, err := json.Marshal(assignment)
	if err != nil {
		return errors.Wrap(err, "marshalling assignment")
	}
	key := assignmentKeyPrefix + topic
	_, err = am.etcd.Put(am.ctx, key, string(data))
	return errors.Wrap(err, "saving assignment to etcd")
}

func (am *AssignmentManager) GetAssignment(topic string) (*TopicAssignment, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	a, ok := am.topics[topic]
	return a, ok
}

func (am *AssignmentManager) AllAssignments() map[string]*TopicAssignment {
	am.mu.RLock()
	defer am.mu.RUnlock()
	cp := make(map[string]*TopicAssignment, len(am.topics))
	for k, v := range am.topics {
		cp[k] = v
	}
	return cp
}

func (am *AssignmentManager) ReassignAll(brokerIDs []string) {
	am.mu.RLock()
	metas := make(map[string]*TopicMeta, len(am.meta))
	for k, v := range am.meta {
		metas[k] = v
	}
	am.mu.RUnlock()

	for topic, meta := range metas {
		rf := meta.ReplicationFactor
		if rf > int32(len(brokerIDs)) {
			rf = int32(len(brokerIDs))
		}

		partitions, err := computeAssignments(brokerIDs, meta.Partitions, rf)
		if err != nil {
			am.logger.Warn(
				"failed computing assignment",
				zap.String("topic", topic),
				zap.Error(err),
			)
			continue
		}

		assignment := TopicAssignment{
			TopicName:         topic,
			Partitions:        partitions,
			ReplicationFactor: rf,
			NumPartitions:     meta.Partitions,
		}

		if err := am.SaveAssignment(topic, assignment); err != nil {
			am.logger.Warn(
				"failed saving reassignment",
				zap.String("topic", topic),
				zap.Error(err),
			)
		} else {
			am.logger.Info(
				"reassigned topic",
				zap.String("topic", topic),
				zap.Int("brokers", len(brokerIDs)),
			)
		}
	}
}

// WaitChange returns a channel that is closed when topic assignments change.
// Callers should call this again after each change to get a fresh channel.
func (am *AssignmentManager) WaitChange() <-chan struct{} {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.notify
}

func (am *AssignmentManager) signalChange() {
	am.mu.Lock()
	close(am.notify)
	am.notify = make(chan struct{})
	am.mu.Unlock()
}

func (am *AssignmentManager) Close() {
	am.cancel()
	am.wg.Wait()
}

func (am *AssignmentManager) watchAssignments() {
	defer am.wg.Done()

	assignCh := am.etcd.Watch(am.ctx, assignmentKeyPrefix, clientv3.WithPrefix())
	metaCh := am.etcd.Watch(am.ctx, topicMetaKeyPrefix, clientv3.WithPrefix())
	for {
		select {
		case <-am.ctx.Done():
			return
		case _, ok := <-assignCh:
			if !ok {
				return
			}
			am.syncAssignmentsFromEtcd()
			am.signalChange()
		case _, ok := <-metaCh:
			if !ok {
				return
			}
			am.syncMetaFromEtcd()
			am.signalChange()
		}
	}
}

func (am *AssignmentManager) syncFromEtcd() {
	am.syncMetaFromEtcd()
	am.syncAssignmentsFromEtcd()
}

func (am *AssignmentManager) syncAssignmentsFromEtcd() {
	resp, err := am.etcd.Get(am.ctx, assignmentKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		am.logger.Warn("failed syncing assignments from etcd", zap.Error(err))
		return
	}

	am.mu.Lock()
	defer am.mu.Unlock()

	etcdTopics := make(map[string]bool)
	for _, kv := range resp.Kvs {
		topic := strings.TrimPrefix(string(kv.Key), assignmentKeyPrefix)
		etcdTopics[topic] = true

		var assignment TopicAssignment
		if err := json.Unmarshal(kv.Value, &assignment); err != nil {
			am.logger.Warn("failed unmarshalling assignment", zap.Error(err))
			continue
		}
		am.topics[topic] = &assignment
	}

	for topic := range am.topics {
		if !etcdTopics[topic] {
			delete(am.topics, topic)
		}
	}
}

func (am *AssignmentManager) syncMetaFromEtcd() {
	resp, err := am.etcd.Get(am.ctx, topicMetaKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		am.logger.Warn("failed syncing meta from etcd", zap.Error(err))
		return
	}

	am.mu.Lock()
	defer am.mu.Unlock()

	etcdMetas := make(map[string]bool)
	for _, kv := range resp.Kvs {
		topic := strings.TrimPrefix(string(kv.Key), topicMetaKeyPrefix)
		etcdMetas[topic] = true

		var meta TopicMeta
		if err := json.Unmarshal(kv.Value, &meta); err != nil {
			am.logger.Warn("failed unmarshalling topic meta", zap.Error(err))
			continue
		}
		am.meta[topic] = &meta
	}

	for topic := range am.meta {
		if !etcdMetas[topic] {
			delete(am.meta, topic)
		}
	}
}
