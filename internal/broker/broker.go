package broker

import (
	"context"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/aalyth/comet/internal/config"
	"github.com/aalyth/comet/internal/storage"
	pb "github.com/aalyth/comet/proto/comet/v1"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// ErrNotLeader is returned when a produce request is sent to a broker that is
// not the leader for the target partition, so that the client can easily
// redirect produces.
type ErrNotLeader struct {
	Partition int32
	LeaderID  string
}

func (e *ErrNotLeader) Error() string {
	return fmt.Sprintf("not leader for partition %d, leader is %s", e.Partition, e.LeaderID)
}

type Broker struct {
	config      *config.Config
	storage     *storage.Storage
	groups      *GroupManager
	registry    *BrokerRegistry
	assignments *AssignmentManager
	peers       *PeerManager
	logger      *zap.Logger

	mu           sync.RWMutex
	roundRobin   map[string]int32
	roundRobinMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type TopicInfo struct {
	Name              string
	Partitions        int32
	ReplicationFactor int32
}

func New(cfg *config.Config, logger *zap.Logger) (*Broker, error) {
	l := logger.Named("broker")

	store, err := storage.New(cfg.DataDir, cfg.SegmentMaxBytes, l)
	if err != nil {
		return nil, errors.Wrap(err, "creating storage")
	}

	brokerID := cfg.BrokerID
	if brokerID == "" {
		brokerID = uuid.New().String()
	}

	advertiseAddr := cfg.AdvertiseAddress
	if advertiseAddr == "" {
		advertiseAddr = cfg.ServerAddress
	}

	var etcdClient *clientv3.Client
	if len(cfg.EtcdEndpoints) > 0 && cfg.EtcdEndpoints[0] != "" {
		etcdClient, err = clientv3.New(
			clientv3.Config{
				Endpoints:   cfg.EtcdEndpoints,
				DialTimeout: 5 * time.Second,
			},
		)
		if err != nil {
			l.Error("etcd unavailable, using in-memory mode", zap.Error(err))
			etcdClient = nil
		} else {
			l.Info("Connected to etcd", zap.Strings("endpoints", cfg.EtcdEndpoints))
		}
	}

	gm := NewGroupManager(
		etcdClient,
		cfg.OffsetCommitCount,
		cfg.OffsetCommitInterval,
		cfg.PollTimeout,
		l,
	)

	registry := NewBrokerRegistry(etcdClient, brokerID, advertiseAddr, l)
	assignments := NewAssignmentManager(etcdClient, l)
	peers := NewPeerManager(registry, l)

	ctx, cancel := context.WithCancel(context.Background())

	b := &Broker{
		config:      cfg,
		storage:     store,
		groups:      gm,
		registry:    registry,
		assignments: assignments,
		peers:       peers,
		logger:      l,
		roundRobin:  make(map[string]int32),
		ctx:         ctx,
		cancel:      cancel,
	}

	if err := registry.Register(); err != nil {
		cancel()
		return nil, errors.Wrap(err, "registering broker")
	}

	// create local storage for partitions this broker owns
	b.ensureLocalPartitions()

	// watch for broker changes and reassign partitions
	if etcdClient != nil {
		b.wg.Add(2)
		go b.watchBrokerChanges()
		go b.watchAssignmentChanges()
	}

	l.Info(
		"broker initialized",
		zap.String("brokerID", brokerID),
		zap.String("dataDir", cfg.DataDir),
		zap.Bool("etcd", etcdClient != nil),
	)

	return b, nil
}

func (b *Broker) BrokerID() string {
	return b.registry.BrokerID()
}

func (b *Broker) Registry() *BrokerRegistry {
	return b.registry
}

func (b *Broker) Assignments() *AssignmentManager {
	return b.assignments
}

func (b *Broker) Storage() *storage.Storage {
	return b.storage
}

func (b *Broker) CreateTopic(name string, partitions, replicationFactor int32) error {
	if partitions <= 0 {
		return fmt.Errorf("partition count must be positive")
	}
	if replicationFactor <= 0 {
		return fmt.Errorf("replication factor must be positive")
	}

	if _, exists := b.assignments.GetTopicMeta(name); exists {
		return fmt.Errorf("topic already exists: %s", name)
	}

	brokerCount := b.registry.BrokerCount()
	if int(replicationFactor) > brokerCount {
		return fmt.Errorf(
			"replication factor %d exceeds broker count %d",
			replicationFactor, brokerCount,
		)
	}

	// save topic metadata
	meta := TopicMeta{
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
	}
	if err := b.assignments.SaveTopicMeta(name, meta); err != nil {
		return errors.Wrap(err, "saving topic metadata")
	}

	// compute and save assignments
	brokerIDs := b.registry.LiveBrokerIDs()
	partAssignments, err := computeAssignments(brokerIDs, partitions, replicationFactor)
	if err != nil {
		return errors.Wrap(err, "computing assignments")
	}

	assignment := TopicAssignment{
		TopicName:         name,
		Partitions:        partAssignments,
		ReplicationFactor: replicationFactor,
		NumPartitions:     partitions,
	}
	if err := b.assignments.SaveAssignment(name, assignment); err != nil {
		return errors.Wrap(err, "saving assignment")
	}

	// create local storage for partitions this broker owns
	b.ensureLocalPartitionsForTopic(name, &assignment)

	b.logger.Info(
		"topic created",
		zap.String("topic", name),
		zap.Int32("partitions", partitions),
		zap.Int32("replicationFactor", replicationFactor),
	)
	return nil
}

func (b *Broker) DeleteTopic(name string) error {
	if _, exists := b.assignments.GetTopicMeta(name); !exists {
		return fmt.Errorf("topic not found: %s", name)
	}

	if err := b.storage.DeleteTopic(name); err != nil {
		// we might not store the topic locally, so it's not an issue
		b.logger.Debug("local topic delete", zap.String("topic", name), zap.Error(err))
	}

	if err := b.assignments.DeleteTopicMeta(name); err != nil {
		return errors.Wrap(err, "deleting topic metadata")
	}

	b.logger.Info("topic deleted", zap.String("topic", name))
	return nil
}

func (b *Broker) ListTopics() ([]*TopicInfo, error) {
	allMeta := b.assignments.AllTopicsMeta()
	allAssignments := b.assignments.AllAssignments()

	topics := make([]*TopicInfo, 0, len(allMeta))
	for name, meta := range allMeta {
		info := &TopicInfo{
			Name:              name,
			Partitions:        meta.Partitions,
			ReplicationFactor: meta.ReplicationFactor,
		}
		if a, ok := allAssignments[name]; ok {
			info.ReplicationFactor = a.ReplicationFactor
		}
		topics = append(topics, info)
	}
	return topics, nil
}

func (b *Broker) GetTopicPartitionCount(name string) (int32, error) {
	meta, ok := b.assignments.GetTopicMeta(name)
	if !ok {
		return 0, fmt.Errorf("topic not found: %s", name)
	}
	return meta.Partitions, nil
}

// Produce handles a batched produce request. For each record, it checks if
// this broker is the leader for the target partition. If so, it writes
// locally and replicates to followers. If not, it returns an ErrNotLeader.
func (b *Broker) Produce(
	topic string, records []*pb.ProduceRecord,
) ([]*pb.ProduceResult, error) {
	assignment, ok := b.assignments.GetAssignment(topic)
	if !ok {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}

	meta, ok := b.assignments.GetTopicMeta(topic)
	if !ok {
		return nil, fmt.Errorf("topic metadata not found: %s", topic)
	}

	results := make([]*pb.ProduceResult, 0, len(records))

	// group records by partition
	type partitionBatch struct {
		partition int32
		records   []*pb.ProduceRecord
	}
	byPartition := make(map[int32]*partitionBatch)

	for _, rec := range records {
		partition := rec.Partition
		if partition < 0 {
			partition = b.selectPartition(topic, rec.Key, meta.Partitions)
		}

		batch, ok := byPartition[partition]
		if !ok {
			batch = &partitionBatch{partition: partition}
			byPartition[partition] = batch
		}
		batch.records = append(batch.records, rec)
	}

	for partition, batch := range byPartition {
		pa, ok := assignment.Partitions[partition]
		if !ok {
			return nil, fmt.Errorf("partition %d not found in assignments", partition)
		}

		if pa.LeaderID != b.registry.BrokerID() {
			return nil, &ErrNotLeader{Partition: partition, LeaderID: pa.LeaderID}
		}

		// write locally
		t, err := b.storage.GetTopic(topic)
		if err != nil {
			return nil, errors.Wrap(err, "getting topic from storage")
		}

		p, err := t.GetPartition(partition)
		if err != nil {
			return nil, errors.Wrap(err, "getting partition from storage")
		}

		var walRecords []*pb.WalRecord
		for _, rec := range batch.records {
			offset, err := p.Append(rec.Key, rec.Value)
			if err != nil {
				return nil, errors.Wrap(err, "appending to partition")
			}
			results = append(
				results, &pb.ProduceResult{
					Partition: partition,
					Offset:    offset,
				},
			)
			walRecords = append(
				walRecords, &pb.WalRecord{
					Offset:    offset,
					Key:       rec.Key,
					Value:     rec.Value,
					Timestamp: time.Now().UnixNano(),
				},
			)
		}

		// replicate to followers in best-effort fashion
		if err := b.replicateToFollowers(topic, partition, pa, walRecords); err != nil {
			b.logger.Warn(
				"replication failed (non-fatal)",
				zap.String("topic", topic),
				zap.Int32("partition", partition),
				zap.Error(err),
			)
		}
	}

	return results, nil
}

// Consume reads records from a local partition. This is the inter-broker
// read path used by Poll for remote partitions.
func (b *Broker) Consume(
	topic string,
	partition int32,
	offset int64,
	maxRecords int,
) ([]*pb.WalRecord, error) {
	t, err := b.storage.GetTopic(topic)
	if err != nil {
		return nil, errors.Wrap(err, "getting topic")
	}

	p, err := t.GetPartition(partition)
	if err != nil {
		return nil, errors.Wrap(err, "getting topic partition")
	}

	records, err := p.ReadFrom(offset, maxRecords)
	if err != nil {
		return nil, errors.Wrap(err, "reading from offset")
	}

	return records, nil
}

// ConsumeOrProxy reads from a partition, proxying to the leader if this
// broker doesn't own it.
func (b *Broker) ConsumeOrProxy(
	topic string,
	partition int32,
	offset int64,
	maxRecords int32,
) ([]*pb.Message, error) {
	assignment, ok := b.assignments.GetAssignment(topic)
	if !ok {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}

	pa, ok := assignment.Partitions[partition]
	if !ok {
		return nil, fmt.Errorf("partition %d not found", partition)
	}

	// check if we own this partition (leader or replica)
	isLocal := false
	for _, rid := range pa.ReplicaIDs {
		if rid == b.registry.BrokerID() {
			isLocal = true
			break
		}
	}

	if isLocal {
		records, err := b.Consume(topic, partition, offset, int(maxRecords))
		if err != nil {
			return nil, err
		}
		return walRecordsToMessages(records, topic, partition), nil
	}

	// proxy to the leader
	client, err := b.peers.GetClient(pa.LeaderID)
	if err != nil {
		return nil, errors.Wrap(err, "getting peer client")
	}

	resp, err := client.Consume(
		b.ctx, &pb.ConsumeRequest{
			Topic:      topic,
			Partition:  partition,
			Offset:     offset,
			MaxRecords: maxRecords,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "proxying consume to leader")
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("leader error: %s", resp.Error)
	}

	return resp.Messages, nil
}

func (b *Broker) JoinGroup(topic, group string) (*GroupMember, int64, error) {
	numPartitions, err := b.GetTopicPartitionCount(topic)
	if err != nil {
		return nil, 0, err
	}
	return b.groups.Join(topic, group, numPartitions)
}

// PollGroup validates membership and returns partition assignments.
func (b *Broker) PollGroup(
	topic, group, memberID string,
) (partitions []int32, rebalance bool, generation int64, err error) {
	return b.groups.Poll(topic, group, memberID)
}

func (b *Broker) LeaveGroup(topic, group, memberID string) error {
	return b.groups.Leave(topic, group, memberID)
}

func (b *Broker) GetMemberOffset(memberID string, partition int32) int64 {
	return b.groups.GetMemberOffset(memberID, partition)
}

func (b *Broker) SetMemberOffset(memberID string, partition int32, offset int64) {
	b.groups.SetMemberOffset(memberID, partition, offset)
}

func (b *Broker) CommitOffset(topic, group string, partition int32, offset int64) {
	b.groups.SetOffset(topic, group, partition, offset)
}

func (b *Broker) Close() error {
	b.logger.Info("shutting down broker")
	b.cancel()
	b.wg.Wait()

	if err := b.peers.Close(); err != nil {
		b.logger.Error("failed closing peers", zap.Error(err))
	}

	if err := b.registry.Close(); err != nil {
		b.logger.Error("failed closing registry", zap.Error(err))
	}

	b.assignments.Close()

	if err := b.groups.Close(); err != nil {
		_ = b.storage.Close()
		return errors.Wrap(err, "closing group manager")
	}
	return b.storage.Close()
}

func (b *Broker) selectPartition(topic string, key []byte, partitionCount int32) int32 {
	if len(key) > 0 {
		return int32(crc32.ChecksumIEEE(key) % uint32(partitionCount))
	}

	b.roundRobinMu.Lock()
	defer b.roundRobinMu.Unlock()

	partition := b.roundRobin[topic]
	b.roundRobin[topic] = (partition + 1) % partitionCount
	return partition
}

func (b *Broker) replicateToFollowers(
	topic string,
	partition int32,
	pa *PartitionAssignment,
	records []*pb.WalRecord,
) error {
	if len(pa.ReplicaIDs) <= 1 {
		// No followers to replicate to.
		return nil
	}

	type result struct {
		brokerID string
		err      error
	}

	resultCh := make(chan result, len(pa.ReplicaIDs)-1)

	for _, replicaID := range pa.ReplicaIDs {
		if replicaID == b.registry.BrokerID() {
			continue // skip self (leader)
		}

		go func(rid string) {
			client, err := b.peers.GetClient(rid)
			if err != nil {
				resultCh <- result{rid, err}
				return
			}

			ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
			defer cancel()

			resp, err := client.Replicate(
				ctx, &pb.ReplicateRequest{
					Topic:     topic,
					Partition: partition,
					Records:   records,
				},
			)
			if err != nil {
				resultCh <- result{rid, err}
				return
			}
			if !resp.Success {
				resultCh <- result{rid, fmt.Errorf(
					"follower error: %s", resp.Error,
				)}
				return
			}
			resultCh <- result{rid, nil}
		}(replicaID)
	}

	// wait for all followers
	var firstErr error
	for i := 0; i < len(pa.ReplicaIDs)-1; i++ {
		res := <-resultCh
		if res.err != nil && firstErr == nil {
			firstErr = fmt.Errorf("replication to %s failed: %w", res.brokerID, res.err)
			b.logger.Warn(
				"Follower replication failed",
				zap.String("broker", res.brokerID),
				zap.Error(res.err),
			)
		}
	}

	return firstErr
}

// watchBrokerChanges listens for broker set changes and reassigns partitions.
func (b *Broker) watchBrokerChanges() {
	defer b.wg.Done()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-b.registry.WaitChange():
			brokerIDs := b.registry.LiveBrokerIDs()
			b.logger.Info(
				"Broker set changed, reassigning partitions",
				zap.Int("brokers", len(brokerIDs)),
			)
			b.assignments.ReassignAll(brokerIDs)
			b.ensureLocalPartitions()
		}
	}
}

// watchAssignmentChanges listens for topic assignment changes (e.g. a new
// topic created by another broker) and creates local storage for any
// partitions this broker now owns.
func (b *Broker) watchAssignmentChanges() {
	defer b.wg.Done()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-b.assignments.WaitChange():
			b.logger.Info("assignment change detected, ensuring local partitions")
			b.ensureLocalPartitions()
		}
	}
}

// ensureLocalPartitions creates local storage for all partitions this broker
// is assigned to (as leader or replica)
func (b *Broker) ensureLocalPartitions() {
	allAssignments := b.assignments.AllAssignments()
	for topic, assignment := range allAssignments {
		b.ensureLocalPartitionsForTopic(topic, assignment)
	}
}

func (b *Broker) ensureLocalPartitionsForTopic(topic string, assignment *TopicAssignment) {
	for partID, pa := range assignment.Partitions {
		owns := false
		for _, rid := range pa.ReplicaIDs {
			if rid == b.registry.BrokerID() {
				owns = true
				break
			}
		}
		if !owns {
			continue
		}

		if err := b.storage.EnsurePartition(topic, partID); err != nil {
			b.logger.Warn(
				"Failed creating local partition",
				zap.String("topic", topic),
				zap.Int32("partition", partID),
				zap.Error(err),
			)
		}
	}
}

func walRecordsToMessages(records []*pb.WalRecord, topic string, partition int32) []*pb.Message {
	msgs := make([]*pb.Message, len(records))
	for i, r := range records {
		msgs[i] = &pb.Message{
			Offset:    r.Offset,
			Key:       r.Key,
			Value:     r.Value,
			Timestamp: r.Timestamp,
			Partition: partition,
			Topic:     topic,
		}
	}
	return msgs
}
