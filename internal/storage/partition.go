package storage

import (
	"sort"
	"sync"
	"time"

	pb "github.com/aalyth/comet/proto/comet/v1"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Partition struct {
	id             int32
	dir            string
	segments       []*WalSegment
	nextOffset     int64
	maxSegmentSize int64
	mu             sync.RWMutex
	logger         *zap.Logger
}

func NewPartition(
	dir string,
	id int32,
	maxSegmentSize int64,
	logger *zap.Logger,
) (*Partition, error) {
	l := logger.With(zap.Int32("partition", id))

	p := &Partition{
		id:             id,
		dir:            dir,
		segments:       make([]*WalSegment, 0),
		nextOffset:     0,
		maxSegmentSize: maxSegmentSize,
		logger:         l,
	}

	segment, err := NewWalSegment(dir, 0, l)
	if err != nil {
		return nil, errors.Wrap(err, "creating initial segment")
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
	if activeSegment.Size() >= p.maxSegmentSize {
		p.logger.Info("Building new segment", zap.Int64("newBaseOffset", offset))

		newSegment, err := NewWalSegment(p.dir, offset, p.logger)
		if err != nil {
			return 0, errors.Wrap(err, "creating new segment")
		}

		p.segments = append(p.segments, newSegment)
		activeSegment = newSegment
	}

	if err := activeSegment.Append(record); err != nil {
		return 0, errors.Wrap(err, "appending to segment")
	}

	return offset, nil
}

func (p *Partition) ReadAt(offset int64) (*pb.WalRecord, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	segmentIdx := p.findSegment(offset)
	if segmentIdx == -1 {
		return nil, ErrSegmentNotFound
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
		return nil, ErrSegmentNotFound
	}

	records := make([]*pb.WalRecord, 0, count)
	currentOffset := startOffset
	for segmentIdx < len(p.segments) && len(records) < count && currentOffset < p.nextOffset {
		record, err := p.segments[segmentIdx].ReadAt(currentOffset)
		if errors.Is(err, ErrRecordNotFound) {
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
		len(p.segments),
		func(i int) bool {
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
