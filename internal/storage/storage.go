package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	ErrSegmentNotFound = fmt.Errorf("partition segment not found")
	ErrRecordNotFound  = fmt.Errorf("record not found")
)

type Storage struct {
	dataDir         string
	segmentMaxBytes int64
	mu              sync.RWMutex
	topics          map[string]*Topic
	logger          *zap.Logger
}

type Topic struct {
	name       string
	partitions map[int32]*Partition
	mu         sync.RWMutex
}

const (
	defaultFolderPermissions = 0755
)

func New(dataDir string, segmentMaxBytes int64, logger *zap.Logger) (*Storage, error) {
	l := logger.Named("storage")

	if err := os.MkdirAll(dataDir, defaultFolderPermissions); err != nil {
		return nil, errors.Wrap(err, "creating data dir")
	}

	l.Info("Storage initialized", zap.String("dataDir", dataDir))
	return &Storage{
		dataDir:         dataDir,
		segmentMaxBytes: segmentMaxBytes,
		topics:          make(map[string]*Topic),
		logger:          l,
	}, nil
}

func (s *Storage) CreateTopic(name string, partitions int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[name]; exists {
		return fmt.Errorf("topic already exists: %s", name)
	}

	topic := &Topic{
		name:       name,
		partitions: make(map[int32]*Partition),
	}

	for i := int32(0); i < partitions; i++ {
		partitionDir := filepath.Join(s.dataDir, name, fmt.Sprintf("%d", i))
		if err := os.MkdirAll(partitionDir, defaultFolderPermissions); err != nil {
			return errors.Wrap(err, "creating partition dir")
		}

		partition, err := NewPartition(partitionDir, i, s.segmentMaxBytes, s.logger)
		if err != nil {
			return errors.Wrapf(err, "creating partition %d", i)
		}

		topic.partitions[i] = partition
	}

	s.topics[name] = topic

	s.logger.Info(
		"Topic created",
		zap.String("topic", name),
		zap.Int32("partitions", partitions),
	)

	return nil
}

func (s *Storage) DeleteTopic(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	topic, exists := s.topics[name]
	if !exists {
		return fmt.Errorf("topic not found: %s", name)
	}

	for _, partition := range topic.partitions {
		if err := partition.Close(); err != nil {
			return fmt.Errorf("close partition: %w", err)
		}
	}

	delete(s.topics, name)

	topicDir := filepath.Join(s.dataDir, name)
	if err := os.RemoveAll(topicDir); err != nil {
		return err
	}

	s.logger.Info("Topic deleted", zap.String("topic", name))
	return nil
}

func (s *Storage) ListTopics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics := make([]string, 0, len(s.topics))
	for name := range s.topics {
		topics = append(topics, name)
	}
	return topics
}

func (s *Storage) GetTopic(name string) (*Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topic, exists := s.topics[name]
	if !exists {
		return nil, fmt.Errorf("topic not found: %s", name)
	}
	return topic, nil
}

func (t *Topic) GetPartition(partitionID int32) (*Partition, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	partition, exists := t.partitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition not found: %d", partitionID)
	}
	return partition, nil
}

func (t *Topic) PartitionCount() int32 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return int32(len(t.partitions))
}

func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, topic := range s.topics {
		for _, partition := range topic.partitions {
			if err := partition.Close(); err != nil {
				return err
			}
		}
	}

	s.logger.Info("Storage closed")
	return nil
}
