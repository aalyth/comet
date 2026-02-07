package broker

import (
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/aalyth/comet/internal/config"
	"github.com/aalyth/comet/internal/storage"
	pb "github.com/aalyth/comet/proto/comet/v1"
)

type Broker struct {
	config       *config.Config
	storage      *storage.Storage
	mu           sync.RWMutex
	roundRobin   map[string]int32
	roundRobinMu sync.Mutex
}

type TopicInfo struct {
	Name       string
	Partitions int32
}

func New(cfg *config.Config) (*Broker, error) {
	store, err := storage.New(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("create storage: %w", err)
	}

	return &Broker{
		config:     cfg,
		storage:    store,
		roundRobin: make(map[string]int32),
	}, nil
}

func (b *Broker) CreateTopic(name string, partitions int32) error {
	if partitions <= 0 {
		return fmt.Errorf("partition count must be positive")
	}

	return b.storage.CreateTopic(name, partitions)
}

func (b *Broker) DeleteTopic(name string) error {
	return b.storage.DeleteTopic(name)
}

func (b *Broker) ListTopics() ([]*TopicInfo, error) {
	names := b.storage.ListTopics()
	topics := make([]*TopicInfo, 0, len(names))

	for _, name := range names {
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

func (b *Broker) Produce(topic string, key, value []byte) (int32, int64, error) {
	t, err := b.storage.GetTopic(topic)
	if err != nil {
		return 0, 0, fmt.Errorf("get topic: %w", err)
	}

	partition := b.selectPartition(topic, key, t.PartitionCount())

	p, err := t.GetPartition(partition)
	if err != nil {
		return 0, 0, fmt.Errorf("get partition: %w", err)
	}

	offset, err := p.Append(key, value)
	if err != nil {
		return 0, 0, fmt.Errorf("append: %w", err)
	}

	return partition, offset, nil
}

func (b *Broker) selectPartition(topic string, key []byte, partitionCount int32) int32 {
	if len(key) > 0 {
		h := fnv.New32a()
		h.Write(key)
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
		return nil, fmt.Errorf("get topic: %w", err)
	}

	p, err := t.GetPartition(partition)
	if err != nil {
		return nil, fmt.Errorf("get partition: %w", err)
	}

	records, err := p.ReadFrom(offset, count)
	if err != nil {
		return nil, fmt.Errorf("read from: %w", err)
	}

	return records, nil
}

func (b *Broker) Close() error {
	return b.storage.Close()
}
