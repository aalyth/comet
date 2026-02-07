package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrInvalidOffset = errors.New("invalid offset")
)

type Storage struct {
	dataDir string
	mu      sync.RWMutex
	topics  map[string]*Topic
}

type Topic struct {
	name       string
	partitions map[int32]*Partition
	mu         sync.RWMutex
}

const (
	defaultFolderPermissions = 0755
)

func New(dataDir string) (*Storage, error) {
	if err := os.MkdirAll(dataDir, defaultFolderPermissions); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	return &Storage{
		dataDir: dataDir,
		topics:  make(map[string]*Topic),
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
			return fmt.Errorf("create partition dir: %w", err)
		}

		partition, err := NewPartition(partitionDir, i)
		if err != nil {
			return fmt.Errorf("create partition %d: %w", i, err)
		}

		topic.partitions[i] = partition
	}

	s.topics[name] = topic
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
	return os.RemoveAll(topicDir)
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
	return nil
}
