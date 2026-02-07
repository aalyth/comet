package storage

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	pb "github.com/aalyth/comet/proto/comet/v1"
)

const maxSegmentSize = 1024 * 1024 * 1024

type Partition struct {
	id         int32
	dir        string
	segments   []*WalSegment
	nextOffset int64
	mu         sync.RWMutex
}

func NewPartition(dir string, id int32) (*Partition, error) {
	p := &Partition{
		id:         id,
		dir:        dir,
		segments:   make([]*WalSegment, 0),
		nextOffset: 0,
	}

	segment, err := NewWalSegment(dir, 0)
	if err != nil {
		return nil, fmt.Errorf("create initial segment: %w", err)
	}

	p.segments = append(p.segments, segment)
	return p, nil
}

func (p *Partition) Append(key, value []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	offset := p.nextOffset
	p.nextOffset++

	record := &pb.WalRecord{
		Offset:    offset,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}

	activeSegment := p.segments[len(p.segments)-1]
	if activeSegment.Size() >= maxSegmentSize {
		newSegment, err := NewWalSegment(p.dir, offset)
		if err != nil {
			return 0, fmt.Errorf("create new segment: %w", err)
		}
		p.segments = append(p.segments, newSegment)
		activeSegment = newSegment
	}

	if err := activeSegment.Append(record); err != nil {
		return 0, fmt.Errorf("append to segment: %w", err)
	}

	return offset, nil
}

func (p *Partition) ReadAt(offset int64) (*pb.WalRecord, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	segmentIdx := p.findSegment(offset)
	if segmentIdx == -1 {
		return nil, ErrNotFound
	}

	return p.segments[segmentIdx].ReadAt(offset)
}

func (p *Partition) ReadFrom(startOffset int64, count int) ([]*pb.WalRecord, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if startOffset >= p.nextOffset {
		return nil, nil
	}

	segmentIdx := p.findSegment(startOffset)
	if segmentIdx == -1 {
		return nil, ErrNotFound
	}

	records := make([]*pb.WalRecord, 0, count)
	currentOffset := startOffset
	for segmentIdx < len(p.segments) && len(records) < count && currentOffset < p.nextOffset {
		record, err := p.segments[segmentIdx].ReadAt(currentOffset)
		if errors.Is(err, ErrNotFound) {
			segmentIdx++
			continue
		}

		if err != nil {
			return nil, err
		}

		records = append(records, record)
		currentOffset++
	}

	return records, nil
}

func (p *Partition) findSegment(offset int64) int {
	idx := sort.Search(
		len(p.segments), func(i int) bool {
			return p.segments[i].baseOffset > offset
		},
	)
	return idx - 1
}

func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, segment := range p.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}
