package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/aalyth/comet/internal/config"
	"github.com/aalyth/comet/internal/logging"
	pb "github.com/aalyth/comet/proto/comet/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startTestServer(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.ServerAddress = lis.Addr().String()

	logger, _ := logging.New("info")
	srv, err := New(cfg, logger)
	require.NoError(t, err)

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
		},
	)

	return lis.Addr().String()
}

func dialServer(t *testing.T, addr string) pb.BrokerServiceClient {
	t.Helper()

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return pb.NewBrokerServiceClient(conn)
}

// drains a consume stream and returns all messages.
func recvAll(stream pb.BrokerService_ConsumeClient) ([]*pb.Message, error) {
	var messages []*pb.Message
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return messages, nil
		}
		if err != nil {
			return messages, err
		}
		if resp.Error != "" {
			return messages, fmt.Errorf("stream error: %s", resp.Error)
		}
		if resp.Message != nil {
			messages = append(messages, resp.Message)
		}
	}
}

// --- Topics management ---

func TestGRPCCreateTopic(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	resp, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "test",
			Partitions: 3,
		},
	)
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Empty(t, resp.Error)
}

func TestGRPCCreateDuplicateTopic(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	_, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "dup",
			Partitions: 1,
		},
	)
	require.NoError(t, err)

	resp, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "dup",
			Partitions: 1,
		},
	)
	require.NoError(t, err)
	assert.False(t, resp.Success)
	assert.NotEmpty(t, resp.Error)
}

func TestGRPCDeleteTopic(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	_, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "del",
			Partitions: 1,
		},
	)
	require.NoError(t, err)

	resp, err := client.DeleteTopic(
		context.Background(), &pb.DeleteTopicRequest{
			Name: "del",
		},
	)
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Empty(t, resp.Error)

	listResp, err := client.ListTopics(context.Background(), &pb.ListTopicsRequest{})
	require.NoError(t, err)
	assert.Empty(t, listResp.Topics)
}

func TestGRPCDeleteNonExistent(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	resp, err := client.DeleteTopic(
		context.Background(), &pb.DeleteTopicRequest{
			Name: "ghost",
		},
	)
	require.NoError(t, err)
	assert.False(t, resp.Success)
	assert.NotEmpty(t, resp.Error)
}

func TestGRPCListTopics(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	expected := map[string]int32{
		"alpha": 1,
		"beta":  3,
		"gamma": 5,
	}

	for name, parts := range expected {
		_, err := client.CreateTopic(
			context.Background(), &pb.CreateTopicRequest{
				Name:       name,
				Partitions: parts,
			},
		)
		require.NoError(t, err)
	}

	resp, err := client.ListTopics(context.Background(), &pb.ListTopicsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Topics, 3)

	got := make(map[string]int32)
	for _, ti := range resp.Topics {
		got[ti.Name] = ti.Partitions
	}
	assert.Equal(t, expected, got)
}

// --- Produce / Consume ---

func TestGRPCProduce(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	_, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "prod",
			Partitions: 1,
		},
	)
	require.NoError(t, err)

	resp, err := client.Produce(
		context.Background(), &pb.ProduceRequest{
			Topic: "prod",
			Key:   []byte("k1"),
			Value: []byte("v1"),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, resp.Error)
	assert.Equal(t, int64(0), resp.Offset)
}

func TestGRPCProduceNonExistent(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	resp, err := client.Produce(
		context.Background(), &pb.ProduceRequest{
			Topic: "ghost",
			Key:   []byte("k"),
			Value: []byte("v"),
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Error)
}

func TestGRPCConsume(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	_, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "consume",
			Partitions: 1,
		},
	)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		_, err := client.Produce(
			context.Background(), &pb.ProduceRequest{
				Topic: "consume",
				Key:   []byte(fmt.Sprintf("k%d", i)),
				Value: []byte(fmt.Sprintf("v%d", i)),
			},
		)
		require.NoError(t, err)
	}

	stream, err := client.Consume(
		context.Background(), &pb.ConsumeRequest{
			Topic:     "consume",
			Partition: 0,
			Offset:    0,
		},
	)
	require.NoError(t, err)

	messages, err := recvAll(stream)
	require.NoError(t, err)
	require.Len(t, messages, 5)

	for i, msg := range messages {
		assert.Equal(t, int64(i), msg.Offset)
		assert.Equal(t, fmt.Sprintf("k%d", i), string(msg.Key))
		assert.Equal(t, fmt.Sprintf("v%d", i), string(msg.Value))
		assert.Greater(t, msg.Timestamp, int64(0))
	}
}

func TestGRPCConsumeEmpty(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	_, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "empty",
			Partitions: 1,
		},
	)
	require.NoError(t, err)

	stream, err := client.Consume(
		context.Background(), &pb.ConsumeRequest{
			Topic:     "empty",
			Partition: 0,
			Offset:    0,
		},
	)
	require.NoError(t, err)

	messages, err := recvAll(stream)
	require.NoError(t, err)
	assert.Empty(t, messages)
}

func TestGRPCConsumeFromOffset(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	_, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "offset",
			Partitions: 1,
		},
	)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := client.Produce(
			context.Background(), &pb.ProduceRequest{
				Topic: "offset",
				Value: []byte(fmt.Sprintf("v%d", i)),
			},
		)
		require.NoError(t, err)
	}

	stream, err := client.Consume(
		context.Background(), &pb.ConsumeRequest{
			Topic:     "offset",
			Partition: 0,
			Offset:    5,
		},
	)
	require.NoError(t, err)

	messages, err := recvAll(stream)
	require.NoError(t, err)
	require.Len(t, messages, 5)

	for i, msg := range messages {
		assert.Equal(t, int64(5+i), msg.Offset)
	}
}

// --- Context / cancellation ---

func TestGRPCConsumeContextCancel(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	_, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "cancel",
			Partitions: 1,
		},
	)
	require.NoError(t, err)

	// produce some data so there's something to stream
	for i := 0; i < 50; i++ {
		_, err := client.Produce(
			context.Background(), &pb.ProduceRequest{
				Topic: "cancel",
				Value: []byte(fmt.Sprintf("v%d", i)),
			},
		)
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	stream, err := client.Consume(
		ctx, &pb.ConsumeRequest{
			Topic:     "cancel",
			Partition: 0,
			Offset:    0,
		},
	)
	require.NoError(t, err)

	// read one message then cancel
	_, err = stream.Recv()
	require.NoError(t, err)

	cancel()

	// subsequent reads should eventually fail
	for {
		_, err := stream.Recv()
		if err != nil {
			break
		}
	}
}

func TestGRPCProduceContextTimeout(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	_, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "timeout",
			Partitions: 1,
		},
	)
	require.NoError(t, err)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err = client.Produce(
		ctx, &pb.ProduceRequest{
			Topic: "timeout",
			Value: []byte("v"),
		},
	)
	assert.Error(t, err)
}

// --- Streaming ---

func TestGRPCConsumeLargeBatch(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	_, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "large",
			Partitions: 1,
		},
	)
	require.NoError(t, err)

	for i := 0; i < 200; i++ {
		_, err := client.Produce(
			context.Background(), &pb.ProduceRequest{
				Topic: "large",
				Value: []byte(fmt.Sprintf("v%d", i)),
			},
		)
		require.NoError(t, err)
	}

	// server uses batchSize=100 internally, so a single consume stream
	// should return at most 100 messages
	stream, err := client.Consume(
		context.Background(), &pb.ConsumeRequest{
			Topic:     "large",
			Partition: 0,
			Offset:    0,
		},
	)
	require.NoError(t, err)

	messages, err := recvAll(stream)
	require.NoError(t, err)
	assert.Equal(
		t, 100, len(messages), "single consume should return at most batchSize messages",
	)
}

func TestGRPCConsumeInvalidPartition(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	_, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "parts",
			Partitions: 2,
		},
	)
	require.NoError(t, err)

	stream, err := client.Consume(
		context.Background(), &pb.ConsumeRequest{
			Topic:     "parts",
			Partition: 99,
			Offset:    0,
		},
	)
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Error, "should receive error for invalid partition")
}
