package client

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/aalyth/comet/internal/config"
	"github.com/aalyth/comet/internal/server"
	pb "github.com/aalyth/comet/proto/comet/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startIntegrationServer(t *testing.T) string {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "comet-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("create listener: %v", err)
	}

	cfg := config.Default()
	cfg.DataDir = tmpDir
	cfg.ServerAddress = lis.Addr().String()

	srv, err := server.New(cfg)
	if err != nil {
		lis.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("create server: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBrokerServiceServer(grpcServer, srv)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("server error: %v", err)
		}
	}()

	t.Cleanup(
		func() {
			grpcServer.GracefulStop()
			srv.Stop()
			os.RemoveAll(tmpDir)
		},
	)

	return lis.Addr().String()
}

func newTestProducer(t *testing.T, addr string, opts ...func(*ProducerConfig)) *Producer {
	t.Helper()

	cfg := DefaultProducerConfig(addr)
	cfg.BufferSize = 100
	cfg.FlushInterval = 50 * time.Millisecond
	cfg.BatchSize = 50

	for _, opt := range opts {
		opt(&cfg)
	}

	p, err := NewProducer(cfg)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}

	t.Cleanup(
		func() {
			p.Close()
		},
	)

	return p
}

func newTestConsumer(t *testing.T, addr string, opts ...func(*ConsumerConfig)) *Consumer {
	t.Helper()

	cfg := DefaultConsumerConfig(addr)
	cfg.PollInterval = 50 * time.Millisecond
	cfg.ChannelBuffer = 100
	cfg.BackoffMin = 10 * time.Millisecond
	cfg.BackoffMax = 100 * time.Millisecond

	for _, opt := range opts {
		opt(&cfg)
	}

	c, err := NewConsumer(cfg)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	t.Cleanup(
		func() {
			c.Close()
		},
	)

	return c
}

func collectMessages(ch <-chan *Message, n int, timeout time.Duration) ([]*Message, error) {
	messages := make([]*Message, 0, n)
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for i := 0; i < n; i++ {
		select {
		case msg := <-ch:
			if msg == nil {
				return messages, fmt.Errorf(
					"channel closed after %d/%d messages", i, n,
				)
			}
			messages = append(messages, msg)
		case <-timer.C:
			return messages, fmt.Errorf("timeout after %d/%d messages", i, n)
		}
	}

	return messages, nil
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("timeout waiting for condition")
}

func createTopic(t *testing.T, addr, topic string, partitions int32) {
	t.Helper()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to server: %v", err)
	}
	defer conn.Close()

	client := pb.NewBrokerServiceClient(conn)
	resp, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       topic,
			Partitions: partitions,
		},
	)
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("create topic error: %s", resp.Error)
	}
}

type MessageExpectation struct {
	Key       *string
	Value     *string
	Offset    *int64
	Topic     *string
	Partition *int32
}

func Expect(key, value string, offset int64, topic string) MessageExpectation {
	return MessageExpectation{
		Key:    &key,
		Value:  &value,
		Offset: &offset,
		Topic:  &topic,
	}
}

func (e MessageExpectation) WithPartition(partition int32) MessageExpectation {
	e.Partition = &partition
	return e
}

func (e MessageExpectation) WithOffset(offset int64) MessageExpectation {
	e.Offset = &offset
	return e
}

func (e MessageExpectation) WithTopic(topic string) MessageExpectation {
	e.Topic = &topic
	return e
}

func assertMessage(t *testing.T, msg *Message, expected MessageExpectation) {
	t.Helper()
	require.NotNil(t, msg, "message should not be nil")

	if expected.Key != nil {
		assert.Equal(t, *expected.Key, string(msg.Key), "key mismatch")
	}
	if expected.Value != nil {
		assert.Equal(t, *expected.Value, string(msg.Value), "value mismatch")
	}
	if expected.Offset != nil {
		assert.Equal(t, *expected.Offset, msg.Offset, "offset mismatch")
	}
	if expected.Topic != nil {
		assert.Equal(t, *expected.Topic, msg.Topic, "topic mismatch")
	}
	if expected.Partition != nil {
		assert.Equal(t, *expected.Partition, msg.Partition, "partition mismatch")
	}

	assert.Greater(t, msg.Timestamp, int64(0), "timestamp should be positive")
}

func assertMessages(t *testing.T, messages []*Message, expectations []MessageExpectation) {
	t.Helper()
	require.Len(t, messages, len(expectations), "message count mismatch")
	for i, expected := range expectations {
		assertMessage(t, messages[i], expected)
	}
}

func assertAllMessagesFromSamePartition(t *testing.T, messages []*Message) {
	t.Helper()
	require.NotEmpty(t, messages, "messages slice should not be empty")
	expectedPartition := messages[0].Partition
	for i, msg := range messages {
		assert.Equal(
			t, expectedPartition, msg.Partition,
			"message %d should be from partition %d", i, expectedPartition,
		)
	}
}
