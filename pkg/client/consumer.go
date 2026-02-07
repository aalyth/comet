package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	pb "github.com/aalyth/comet/proto/comet/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type subscription struct {
	ch     chan *Message
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type Consumer struct {
	config ConsumerConfig
	conn   *grpc.ClientConn
	client pb.BrokerServiceClient

	mu            sync.Mutex
	subscriptions map[string]*subscription

	rootCtx context.Context
	cancel  context.CancelFunc
}

func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	conn, err := grpc.NewClient(
		config.BrokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to broker: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		config:        config,
		conn:          conn,
		client:        pb.NewBrokerServiceClient(conn),
		subscriptions: make(map[string]*subscription),
		rootCtx:       ctx,
		cancel:        cancel,
	}, nil
}

func (c *Consumer) Subscribe(topic string, handler MessageHandler) error {
	ch, err := c.subscribe(topic)
	if err != nil {
		return err
	}

	go func() {
		for msg := range ch {
			handler(msg)
		}
	}()

	return nil
}

func (c *Consumer) SubscribeChan(topic string) (<-chan *Message, error) {
	return c.subscribe(topic)
}

func (c *Consumer) Unsubscribe(topic string) error {
	c.mu.Lock()
	sub, ok := c.subscriptions[topic]
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("not subscribed to topic %q", topic)
	}
	delete(c.subscriptions, topic)
	c.mu.Unlock()

	sub.cancel()
	sub.wg.Wait()
	close(sub.ch)

	return nil
}

func (c *Consumer) Close() error {
	c.cancel()

	c.mu.Lock()
	subs := make(map[string]*subscription, len(c.subscriptions))
	for k, v := range c.subscriptions {
		subs[k] = v
	}
	c.subscriptions = make(map[string]*subscription)
	c.mu.Unlock()

	for _, sub := range subs {
		sub.wg.Wait()
		close(sub.ch)
	}

	return c.conn.Close()
}

func (c *Consumer) getSub(topic string) *subscription {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subscriptions[topic]
}

func (c *Consumer) subscribe(topic string) (chan *Message, error) {
	c.mu.Lock()
	if _, ok := c.subscriptions[topic]; ok {
		c.mu.Unlock()
		return nil, fmt.Errorf("already subscribed to topic %q", topic)
	}
	c.mu.Unlock()

	resp, err := c.client.ListTopics(c.rootCtx, &pb.ListTopicsRequest{})
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}

	var partitions int32
	for _, t := range resp.Topics {
		if t.Name == topic {
			partitions = t.Partitions
			break
		}
	}
	if partitions == 0 {
		return nil, fmt.Errorf("topic %q not found", topic)
	}

	subCtx, subCancel := context.WithCancel(c.rootCtx)
	ch := make(chan *Message, c.config.ChannelBuffer)

	sub := &subscription{
		ch:     ch,
		cancel: subCancel,
	}

	c.mu.Lock()
	if _, ok := c.subscriptions[topic]; ok {
		c.mu.Unlock()
		subCancel()
		return nil, fmt.Errorf("already subscribed to topic %q", topic)
	}
	c.subscriptions[topic] = sub
	c.mu.Unlock()

	for p := int32(0); p < partitions; p++ {
		sub.wg.Add(1)
		go c.pollPartition(subCtx, sub, topic, p)
	}

	return ch, nil
}

func (c *Consumer) pollPartition(ctx context.Context, sub *subscription, topic string, partition int32) {
	defer sub.wg.Done()

	offset := c.config.InitialOffset
	backoff := c.config.BackoffMin

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		stream, err := c.client.Consume(ctx, &pb.ConsumeRequest{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			backoff = c.sleep(ctx, backoff)
			continue
		}

		received := 0
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				break
			}
			if resp.Error != "" {
				break
			}

			msg := resp.Message
			if msg == nil {
				continue
			}

			m := &Message{
				Topic:     topic,
				Partition: partition,
				Offset:    msg.Offset,
				Key:       msg.Key,
				Value:     msg.Value,
				Timestamp: msg.Timestamp,
			}

			select {
			case sub.ch <- m:
			case <-ctx.Done():
				return
			}

			offset = msg.Offset + 1
			received++
		}

		if received > 0 {
			backoff = c.config.BackoffMin
			continue
		}

		backoff = c.sleep(ctx, backoff)
	}
}

func (c *Consumer) sleep(ctx context.Context, backoff time.Duration) time.Duration {
	timer := time.NewTimer(backoff)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
	}

	next := time.Duration(float64(backoff) * c.config.BackoffMultiplier)
	if next > c.config.BackoffMax {
		next = c.config.BackoffMax
	}
	return next
}
