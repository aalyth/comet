package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	pb "github.com/aalyth/comet/proto/comet/v1"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type pendingMessage struct {
	key   []byte
	value []byte
}

// TopicProducer is a buffered, async producer bound to a single topic. It
// automatically creates the topic on startup (ignoring "already exists" errors).
type TopicProducer struct {
	config ProducerConfig
	conn   *grpc.ClientConn
	client pb.BrokerServiceClient
	logger *zap.Logger

	mu     sync.Mutex
	buffer []pendingMessage

	flushCh  chan struct{}
	doneCh   chan struct{}
	closeCtx context.Context
	cancel   context.CancelFunc
}

func NewTopicProducer(config ProducerConfig) (*TopicProducer, error) {
	logger := config.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	l := logger.Named("producer")

	conn, err := grpc.NewClient(
		config.BrokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to broker")
	}

	rpc := pb.NewBrokerServiceClient(conn)

	resp, err := rpc.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       config.Topic,
			Partitions: config.Partitions,
		},
	)
	if err != nil {
		conn.Close()
		return nil, errors.Wrap(err, "creating topic")
	}
	if resp.Error != "" && !strings.Contains(resp.Error, "already exists") {
		conn.Close()
		return nil, fmt.Errorf("creating topic: %s", resp.Error)
	}

	l.Info(
		"Topic producer created",
		zap.String("topic", config.Topic),
		zap.Int32("partitions", config.Partitions),
		zap.String("broker", config.BrokerAddress),
	)

	ctx, cancel := context.WithCancel(context.Background())

	p := &TopicProducer{
		config:   config,
		conn:     conn,
		client:   rpc,
		logger:   l,
		buffer:   make([]pendingMessage, 0, config.BufferSize),
		flushCh:  make(chan struct{}, 1),
		doneCh:   make(chan struct{}),
		closeCtx: ctx,
		cancel:   cancel,
	}

	go p.flusher()

	return p, nil
}

func (p *TopicProducer) Send(key, value []byte) error {
	p.mu.Lock()
	if p.closeCtx.Err() != nil {
		p.mu.Unlock()
		return fmt.Errorf("producer is closed")
	}
	p.buffer = append(p.buffer, pendingMessage{key: key, value: value})
	full := len(p.buffer) >= p.config.BufferSize
	p.mu.Unlock()

	if full {
		p.logger.Debug(
			"Buffer is full, flushing",
			zap.Int("size", p.config.BufferSize),
		)
		p.triggerFlush()
	}

	return nil
}

func (p *TopicProducer) Flush() error {
	p.mu.Lock()
	if len(p.buffer) == 0 {
		p.mu.Unlock()
		return nil
	}
	batch := p.buffer
	p.buffer = make([]pendingMessage, 0, p.config.BufferSize)
	p.mu.Unlock()

	p.logger.Debug(
		"Flushing",
		zap.Int("messages", len(batch)),
		zap.String("topic", p.config.Topic),
	)

	var firstErr error
	for i := 0; i < len(batch); i += p.config.BatchSize {
		end := i + p.config.BatchSize
		if end > len(batch) {
			end = len(batch)
		}

		for _, msg := range batch[i:end] {
			resp, err := p.client.Produce(
				context.Background(), &pb.ProduceRequest{
					Topic: p.config.Topic,
					Key:   msg.key,
					Value: msg.value,
				},
			)
			if err != nil && firstErr == nil {
				firstErr = errors.Wrap(err, "producing")
			}
			if err == nil && resp.Error != "" && firstErr == nil {
				firstErr = fmt.Errorf("producing: %s", resp.Error)
			}
		}
	}

	if firstErr != nil {
		p.logger.Warn("Error while flushing", zap.Error(firstErr))
	}

	return firstErr
}

func (p *TopicProducer) Close() error {
	p.logger.Info("Closing producer", zap.String("topic", p.config.Topic))
	p.cancel()
	<-p.doneCh

	if err := p.Flush(); err != nil {
		p.conn.Close()
		return err
	}

	return p.conn.Close()
}

func (p *TopicProducer) triggerFlush() {
	select {
	case p.flushCh <- struct{}{}:
	default:
	}
}

func (p *TopicProducer) flusher() {
	defer close(p.doneCh)

	ticker := time.NewTicker(p.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.Flush()
		case <-p.flushCh:
			p.Flush()
		case <-p.closeCtx.Done():
			return
		}
	}
}
