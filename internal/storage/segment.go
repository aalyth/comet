package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/aalyth/comet/proto/comet/v1"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type WalSegment struct {
	baseOffset int64
	file       *os.File
	size       int64
	index      map[int64]int64
	mu         sync.RWMutex
	logger     *zap.Logger
}

const (
	segmentFilePermissions = 0644
)

func NewWalSegment(dir string, baseOffset int64, logger *zap.Logger) (*WalSegment, error) {
	path := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, segmentFilePermissions)
	if err != nil {
		return nil, errors.Wrap(err, "opening segment file")
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, errors.Wrap(err, "getting segment file stat")
	}

	s := &WalSegment{
		baseOffset: baseOffset,
		file:       file,
		size:       info.Size(),
		index:      make(map[int64]int64),
		logger:     logger,
	}

	if info.Size() > 0 {
		if err := s.buildIndex(); err != nil {
			_ = file.Close()
			return nil, errors.Wrap(err, "building segment file index")
		}
		s.logger.Debug(
			"Rebuilt segment file index",
			zap.Int("entries", len(s.index)),
			zap.Int64("fileSize", info.Size()),
		)
	}

	s.logger.Debug(
		"Segment file opened",
		zap.String("path", path),
		zap.Int64("baseOffset", baseOffset),
	)
	return s, nil
}

func (s *WalSegment) buildIndex() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "seeking beginning")
	}

	for {
		pos, err := s.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return errors.Wrap(err, "getting position")
		}

		record, err := s.readRecord()
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrap(err, "reading record")
		}

		s.index[record.Offset] = pos
	}

	return nil
}

func (s *WalSegment) Append(record *pb.WalRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos, err := s.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return errors.Wrap(err, "getting segment position")
	}

	data, err := proto.Marshal(record)
	if err != nil {
		return errors.Wrap(err, "marshaling record")
	}

	length := uint32(len(data))
	if err := binary.Write(s.file, binary.BigEndian, length); err != nil {
		return errors.Wrap(err, "writing record length")
	}

	if _, err := s.file.Write(data); err != nil {
		return errors.Wrap(err, "writing record data")
	}

	s.index[record.Offset] = pos

	info, err := s.file.Stat()
	if err != nil {
		return errors.Wrap(err, "getting segment file stat")
	}
	s.size = info.Size()

	return nil
}

func (s *WalSegment) ReadAt(offset int64) (*pb.WalRecord, error) {
	s.mu.RLock()
	pos, exists := s.index[offset]
	s.mu.RUnlock()

	if !exists {
		return nil, ErrRecordNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.file.Seek(pos, io.SeekStart); err != nil {
		return nil, errors.Wrap(err, "seeking within segment file")
	}

	return s.readRecord()
}

func (s *WalSegment) readRecord() (*pb.WalRecord, error) {
	var length uint32
	if err := binary.Read(s.file, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(s.file, data); err != nil {
		return nil, errors.Wrap(err, "reading record data")
	}

	record := &pb.WalRecord{}
	if err := proto.Unmarshal(data, record); err != nil {
		return nil, errors.Wrap(err, "unmarshalling record")
	}

	return record, nil
}

func (s *WalSegment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

func (s *WalSegment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}
