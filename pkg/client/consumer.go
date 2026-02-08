package client

import (
	"fmt"
	"io"
	"sync"
	"time"

	pb "github.com/aalyth/comet/proto/comet/v1"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type subscription struct {
	ch     chan *Message
	cancel func()
	wg     sync.WaitGroup
}

// Consumer connects to a broker and consumes messages from topics using
// consumer groups. The broker handles partition assignment and rebalancing
// via the Subscribe RPC. If a topic does not yet exist the consumer waits
// and retries with exponential backoff rather than returning an error.
type Consumer struct {
	config ConsumerConfig
	conn   *grpc.ClientConn
	client pb.BrokerServiceClient
	logger *zap.Logger

	mu            sync.Mutex
	subscriptions map[string]*subscription

	closeCh chan struct{}
	closed  bool
}

func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	logger := config.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	l := logger.Named("consumer")

	conn, err := grpc.NewClient(
		config.BrokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to broker")
	}

	l.Info(
		"Consumer created",
		zap.String("broker", config.BrokerAddress),
		zap.String("group", config.Group),
	)

	return &Consumer{
		config:        config,
		conn:          conn,
		client:        pb.NewBrokerServiceClient(conn),
		logger:        l,
		subscriptions: make(map[string]*subscription),
		closeCh:       make(chan struct{}),
	}, nil
}

// Subscribe starts consuming messages from the given topic using the
// consumer group configured in ConsumerConfig. The handler is called for
// each message received. If the topic doesn't exist yet the consumer will
// wait and retry with backoff.
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

// SubscribeChan starts consuming and returns a channel of messages.
func (c *Consumer) SubscribeChan(topic string) (<-chan *Message, error) {
	return c.subscribe(topic)
}

// Unsubscribe stops consuming from a topic.
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

	c.logger.Info("Unsubscribed", zap.String("topic", topic))
	return nil
}

// Close stops all subscriptions and releases resources.
func (c *Consumer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.closeCh)

	subs := make(map[string]*subscription, len(c.subscriptions))
	for k, v := range c.subscriptions {
		subs[k] = v
	}
	c.subscriptions = make(map[string]*subscription)
	c.mu.Unlock()

	for _, sub := range subs {
		sub.cancel()
		sub.wg.Wait()
		close(sub.ch)
	}

	c.logger.Info("Consumer closed")
	return c.conn.Close()
}

func (c *Consumer) subscribe(topic string) (chan *Message, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, fmt.Errorf("consumer is closed")
	}
	if _, ok := c.subscriptions[topic]; ok {
		c.mu.Unlock()
		return nil, fmt.Errorf("already subscribed to topic %q", topic)
	}

	subCloseCh := make(chan struct{})
	cancelFn := func() { close(subCloseCh) }

	ch := make(chan *Message, c.config.ChannelBuffer)
	sub := &subscription{
		ch:     ch,
		cancel: cancelFn,
	}
	c.subscriptions[topic] = sub
	c.mu.Unlock()

	sub.wg.Add(1)
	go c.consumeLoop(sub, topic, subCloseCh)

	c.logger.Info("Subscribed", zap.String("topic", topic), zap.String("group", c.config.Group))
	return ch, nil
}

func (c *Consumer) consumeLoop(sub *subscription, topic string, stopCh <-chan struct{}) {
	defer sub.wg.Done()

	backoff := c.config.BackoffMin
	for {
		select {
		case <-stopCh:
			return
		case <-c.closeCh:
			return
		default:
		}

		stream, err := c.client.Subscribe(
			newStopContext(stopCh, c.closeCh),
			&pb.SubscribeRequest{
				Topic: topic,
				Group: c.config.Group,
			},
		)
		if err != nil {
			if isStopped(stopCh, c.closeCh) {
				return
			}
			c.logger.Warn(
				"Failed subscribing, retrying",
				zap.String("topic", topic),
				zap.Error(err),
				zap.Duration("backoff", backoff),
			)
			backoff = c.sleep(stopCh, backoff)
			continue
		}

		c.logger.Debug("Stream opened", zap.String("topic", topic))

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				if isStopped(stopCh, c.closeCh) {
					return
				}
				c.logger.Warn(
					"Failed reading from stream, reconnecting",
					zap.String("topic", topic),
					zap.Error(err),
				)
				break
			}

			if resp.Error != "" {
				c.logger.Warn(
					"Server error, retrying",
					zap.String("topic", topic),
					zap.String("error", resp.Error),
				)
				break
			}

			msg := resp.Message
			if msg == nil {
				continue
			}

			m := &Message{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Key:       msg.Key,
				Value:     msg.Value,
				Timestamp: msg.Timestamp,
			}

			select {
			case sub.ch <- m:
				// reset on successful receive
				backoff = c.config.BackoffMin
			case <-stopCh:
				return
			case <-c.closeCh:
				return
			}
		}

		// back off before reconnecting
		backoff = c.sleep(stopCh, backoff)
	}
}

func (c *Consumer) sleep(stopCh <-chan struct{}, backoff time.Duration) time.Duration {
	timer := time.NewTimer(backoff)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-stopCh:
	case <-c.closeCh:
	}

	next := time.Duration(float64(backoff) * c.config.BackoffMultiplier)
	if next > c.config.BackoffMax {
		next = c.config.BackoffMax
	}
	return next
}

type stopContext struct {
	stopCh  <-chan struct{}
	closeCh <-chan struct{}
}

func newStopContext(stopCh, closeCh <-chan struct{}) *stopContext {
	return &stopContext{stopCh: stopCh, closeCh: closeCh}
}

func (s *stopContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (s *stopContext) Value(any) any {
	return nil
}

func (s *stopContext) Err() error {
	select {
	case <-s.stopCh:
		return fmt.Errorf("subscription stopped")
	case <-s.closeCh:
		return fmt.Errorf("consumer closed")
	default:
		return nil
	}
}
func (s *stopContext) Done() <-chan struct{} {
	// Merge both channels into one.
	ch := make(chan struct{})
	go func() {
		select {
		case <-s.stopCh:
		case <-s.closeCh:
		}
		close(ch)
	}()
	return ch
}

func isStopped(stopCh, closeCh <-chan struct{}) bool {
	select {
	case <-stopCh:
		return true
	case <-closeCh:
		return true
	default:
		return false
	}
}
