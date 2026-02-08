package broker

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/aalyth/comet/internal/config"
	"github.com/aalyth/comet/internal/storage"
	pb "github.com/aalyth/comet/proto/comet/v1"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type Broker struct {
	config       *config.Config
	storage      *storage.Storage
	groups       *GroupManager
	logger       *zap.Logger
	mu           sync.RWMutex
	roundRobin   map[string]int32
	roundRobinMu sync.Mutex
}

type TopicInfo struct {
	Name       string
	Partitions int32
}

func New(cfg *config.Config, logger *zap.Logger) (*Broker, error) {
	l := logger.Named("broker")

	store, err := storage.New(cfg.DataDir, cfg.SegmentMaxBytes, l)
	if err != nil {
		return nil, errors.Wrap(err, "creating storage")
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
			l.Warn("etcd unavailable, using in-memory groups", zap.Error(err))
			etcdClient = nil
		} else {
			l.Info("connected to etcd", zap.Strings("endpoints", cfg.EtcdEndpoints))
		}
	}

	gm := NewGroupManager(etcdClient, cfg.OffsetCommitCount, cfg.OffsetCommitInterval, l)
	l.Info(
		"broker initialized",
		zap.String("dataDir", cfg.DataDir),
		zap.Bool("etcd", etcdClient != nil),
	)

	return &Broker{
		config:     cfg,
		storage:    store,
		groups:     gm,
		logger:     l,
		roundRobin: make(map[string]int32),
	}, nil
}

func (b *Broker) CreateTopic(name string, partitions int32) error {
	if partitions <= 0 {
		return fmt.Errorf("partition count must be positive")
	}

	if err := b.storage.CreateTopic(name, partitions); err != nil {
		return err
	}

	b.logger.Info(
		"topic created", zap.String("topic", name), zap.Int32("partitions", partitions),
	)
	return nil
}

func (b *Broker) DeleteTopic(name string) error {
	if err := b.storage.DeleteTopic(name); err != nil {
		return err
	}

	b.logger.Info("topic deleted", zap.String("topic", name))
	return nil
}

func (b *Broker) ListTopics() ([]*TopicInfo, error) {
	topicNames := b.storage.ListTopics()
	topics := make([]*TopicInfo, 0, len(topicNames))
	for _, name := range topicNames {
		topic, err := b.storage.GetTopic(name)
		if err != nil {
			return nil, fmt.Errorf("get topic %s: %w", name, err)
		}

		topics = append(
			topics, &TopicInfo{
				Name:       name,
				Partitions: topic.PartitionCount(),
			},
		)
	}
	return topics, nil
}

func (b *Broker) GetTopicPartitionCount(name string) (int32, error) {
	topic, err := b.storage.GetTopic(name)
	if err != nil {
		return 0, errors.Wrap(err, "getting topic")
	}
	return topic.PartitionCount(), nil
}

func (b *Broker) Produce(topic string, key, value []byte) (int32, int64, error) {
	t, err := b.storage.GetTopic(topic)
	if err != nil {
		return 0, 0, errors.Wrap(err, "getting topic")
	}

	partition := b.selectPartition(topic, key, t.PartitionCount())

	p, err := t.GetPartition(partition)
	if err != nil {
		return 0, 0, errors.Wrap(err, "getting partition")
	}

	offset, err := p.Append(key, value)
	if err != nil {
		return 0, 0, errors.Wrap(err, "appending to partition")
	}

	b.logger.Debug(
		"message produced",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
	)

	return partition, offset, nil
}

func (b *Broker) selectPartition(topic string, key []byte, partitionCount int32) int32 {
	if len(key) > 0 {
		h := fnv.New32a()
		_, _ = h.Write(key)
		return int32(h.Sum32() % uint32(partitionCount))
	}

	b.roundRobinMu.Lock()
	defer b.roundRobinMu.Unlock()

	partition := b.roundRobin[topic]
	b.roundRobin[topic] = (partition + 1) % partitionCount

	return partition
}

func (b *Broker) Consume(
	topic string,
	partition int32,
	offset int64,
	count int,
) ([]*pb.WalRecord, error) {
	t, err := b.storage.GetTopic(topic)
	if err != nil {
		return nil, errors.Wrap(err, "getting topic")
	}

	p, err := t.GetPartition(partition)
	if err != nil {
		return nil, errors.Wrap(err, "getting topic partition")
	}

	records, err := p.ReadFrom(offset, count)
	if err != nil {
		return nil, errors.Wrap(err, "reading from offset")
	}

	return records, nil
}

func (b *Broker) JoinGroup(topic, group string, numPartitions int32) (*GroupMember, func(), error) {
	return b.groups.Join(topic, group, numPartitions)
}

func (b *Broker) GetGroupOffset(topic, group string, partition int32) (int64, error) {
	return b.groups.GetOffset(topic, group, partition)
}

func (b *Broker) SetGroupOffset(topic, group string, partition int32, offset int64) {
	b.groups.SetOffset(topic, group, partition, offset)
}

func (b *Broker) WaitRebalance(topic, group string) <-chan struct{} {
	return b.groups.WaitRebalance(topic, group)
}

func (b *Broker) Close() error {
	b.logger.Info("shutting down broker")

	if err := b.groups.Close(); err != nil {
		_ = b.storage.Close()
		return errors.Wrap(err, "closing group manager")
	}
	return b.storage.Close()
}
