package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/aalyth/comet/proto/comet/v1"
	"google.golang.org/protobuf/proto"
)

type WalSegment struct {
	baseOffset int64
	file       *os.File
	size       int64
	index      map[int64]int64
	mu         sync.RWMutex
}

const (
	segmentFilePermissions = 0644
)

func NewWalSegment(dir string, baseOffset int64) (*WalSegment, error) {
	path := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, segmentFilePermissions)
	if err != nil {
		return nil, fmt.Errorf("open segment file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("stat segment file: %w", err)
	}

	s := &WalSegment{
		baseOffset: baseOffset,
		file:       file,
		size:       info.Size(),
		index:      make(map[int64]int64),
	}

	if info.Size() > 0 {
		if err := s.buildIndex(); err != nil {
			file.Close()
			return nil, fmt.Errorf("build index: %w", err)
		}
	}

	return s, nil
}

func (s *WalSegment) buildIndex() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek: %w", err)
	}

	for {
		pos, err := s.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("get position: %w", err)
		}

		record, err := s.readRecord()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read record: %w", err)
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
		return fmt.Errorf("get position: %w", err)
	}

	data, err := proto.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}

	length := uint32(len(data))
	if err := binary.Write(s.file, binary.BigEndian, length); err != nil {
		return fmt.Errorf("write length: %w", err)
	}

	if _, err := s.file.Write(data); err != nil {
		return fmt.Errorf("write data: %w", err)
	}

	s.index[record.Offset] = pos

	info, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}
	s.size = info.Size()

	return nil
}

func (s *WalSegment) ReadAt(offset int64) (*pb.WalRecord, error) {
	s.mu.RLock()
	pos, exists := s.index[offset]
	s.mu.RUnlock()

	if !exists {
		return nil, ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.file.Seek(pos, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek: %w", err)
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
		return nil, fmt.Errorf("read data: %w", err)
	}

	record := &pb.WalRecord{}
	if err := proto.Unmarshal(data, record); err != nil {
		return nil, fmt.Errorf("unmarshal record: %w", err)
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
