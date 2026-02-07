package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/aalyth/comet/proto/comet/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type pendingMessage struct {
	topic string
	key   []byte
	value []byte
}

type Producer struct {
	config ProducerConfig
	conn   *grpc.ClientConn
	client pb.BrokerServiceClient

	mu     sync.Mutex
	buffer []pendingMessage

	flushCh  chan struct{}
	doneCh   chan struct{}
	closeCtx context.Context
	cancel   context.CancelFunc
}

func NewProducer(config ProducerConfig) (*Producer, error) {
	conn, err := grpc.NewClient(
		config.BrokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to broker: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Producer{
		config:   config,
		conn:     conn,
		client:   pb.NewBrokerServiceClient(conn),
		buffer:   make([]pendingMessage, 0, config.BufferSize),
		flushCh:  make(chan struct{}, 1),
		doneCh:   make(chan struct{}),
		closeCtx: ctx,
		cancel:   cancel,
	}

	go p.flusher()

	return p, nil
}

func (p *Producer) Send(topic string, key, value []byte) error {
	p.mu.Lock()
	if p.closeCtx.Err() != nil {
		p.mu.Unlock()
		return fmt.Errorf("producer is closed")
	}
	p.buffer = append(p.buffer, pendingMessage{topic: topic, key: key, value: value})
	full := len(p.buffer) >= p.config.BufferSize
	p.mu.Unlock()

	if full {
		p.triggerFlush()
	}

	return nil
}

func (p *Producer) Flush() error {
	p.mu.Lock()
	if len(p.buffer) == 0 {
		p.mu.Unlock()
		return nil
	}
	batch := p.buffer
	p.buffer = make([]pendingMessage, 0, p.config.BufferSize)
	p.mu.Unlock()

	var firstErr error
	for i := 0; i < len(batch); i += p.config.BatchSize {
		end := i + p.config.BatchSize
		if end > len(batch) {
			end = len(batch)
		}

		for _, msg := range batch[i:end] {
			resp, err := p.client.Produce(context.Background(), &pb.ProduceRequest{
				Topic: msg.topic,
				Key:   msg.key,
				Value: msg.value,
			})
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("produce: %w", err)
			}
			if err == nil && resp.Error != "" && firstErr == nil {
				firstErr = fmt.Errorf("produce: %s", resp.Error)
			}
		}
	}
	return firstErr
}

func (p *Producer) Close() error {
	p.cancel()
	<-p.doneCh

	if err := p.Flush(); err != nil {
		p.conn.Close()
		return err
	}

	return p.conn.Close()
}

func (p *Producer) triggerFlush() {
	select {
	case p.flushCh <- struct{}{}:
	default:
	}
}

func (p *Producer) flusher() {
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
