package server

import (
	"context"
	"fmt"
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
	cfg.EtcdEndpoints = nil

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

// --- Topics management ---

func TestGRPCCreateTopic(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	resp, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:              "test",
			Partitions:        3,
			ReplicationFactor: 1,
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

// --- Produce / Consume (unary, batched) ---

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
			Records: []*pb.ProduceRecord{
				{Partition: 0, Key: []byte("k1"), Value: []byte("v1")},
			},
		},
	)
	require.NoError(t, err)
	assert.Empty(t, resp.Error)
	require.Len(t, resp.Results, 1)
	assert.Equal(t, int64(0), resp.Results[0].Offset)
}

func TestGRPCProduceBatched(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	_, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "batch",
			Partitions: 1,
		},
	)
	require.NoError(t, err)

	records := make([]*pb.ProduceRecord, 5)
	for i := 0; i < 5; i++ {
		records[i] = &pb.ProduceRecord{
			Partition: 0,
			Key:       []byte(fmt.Sprintf("k%d", i)),
			Value:     []byte(fmt.Sprintf("v%d", i)),
		}
	}

	resp, err := client.Produce(
		context.Background(), &pb.ProduceRequest{
			Topic:   "batch",
			Records: records,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, resp.Error)
	require.Len(t, resp.Results, 5)

	for i, r := range resp.Results {
		assert.Equal(t, int64(i), r.Offset)
	}
}

func TestGRPCProduceNonExistent(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	resp, err := client.Produce(
		context.Background(), &pb.ProduceRequest{
			Topic: "ghost",
			Records: []*pb.ProduceRecord{
				{Partition: 0, Key: []byte("k"), Value: []byte("v")},
			},
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
				Records: []*pb.ProduceRecord{
					{Partition: 0, Key: []byte(fmt.Sprintf(
						"k%d", i,
					)), Value: []byte(fmt.Sprintf("v%d", i))},
				},
			},
		)
		require.NoError(t, err)
	}

	resp, err := client.Consume(
		context.Background(), &pb.ConsumeRequest{
			Topic:      "consume",
			Partition:  0,
			Offset:     0,
			MaxRecords: 100,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, resp.Error)
	require.Len(t, resp.Messages, 5)

	for i, msg := range resp.Messages {
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

	resp, err := client.Consume(
		context.Background(), &pb.ConsumeRequest{
			Topic:      "empty",
			Partition:  0,
			Offset:     0,
			MaxRecords: 100,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, resp.Error)
	assert.Empty(t, resp.Messages)
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

	records := make([]*pb.ProduceRecord, 10)
	for i := 0; i < 10; i++ {
		records[i] = &pb.ProduceRecord{
			Partition: 0,
			Value:     []byte(fmt.Sprintf("v%d", i)),
		}
	}

	_, err = client.Produce(
		context.Background(), &pb.ProduceRequest{
			Topic:   "offset",
			Records: records,
		},
	)
	require.NoError(t, err)

	resp, err := client.Consume(
		context.Background(), &pb.ConsumeRequest{
			Topic:      "offset",
			Partition:  0,
			Offset:     5,
			MaxRecords: 100,
		},
	)
	require.NoError(t, err)
	require.Len(t, resp.Messages, 5)

	for i, msg := range resp.Messages {
		assert.Equal(t, int64(5+i), msg.Offset)
	}
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

	resp, err := client.Consume(
		context.Background(), &pb.ConsumeRequest{
			Topic:      "parts",
			Partition:  99,
			Offset:     0,
			MaxRecords: 10,
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Error, "should receive error for invalid partition")
}

// --- GetMetadata ---

func TestGRPCGetMetadata(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	_, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:              "meta-test",
			Partitions:        3,
			ReplicationFactor: 1,
		},
	)
	require.NoError(t, err)

	resp, err := client.GetMetadata(
		context.Background(), &pb.GetMetadataRequest{},
	)
	require.NoError(t, err)

	// should have at least 1 broker
	assert.GreaterOrEqual(t, len(resp.Brokers), 1)

	// should have 1 topic with 3 partitions
	require.Len(t, resp.Topics, 1)
	assert.Equal(t, "meta-test", resp.Topics[0].Name)
	assert.Len(t, resp.Topics[0].Partitions, 3)
}

func TestGRPCGetMetadataFiltered(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	for _, name := range []string{"t1", "t2", "t3"} {
		_, err := client.CreateTopic(
			context.Background(), &pb.CreateTopicRequest{
				Name:       name,
				Partitions: 1,
			},
		)
		require.NoError(t, err)
	}

	resp, err := client.GetMetadata(
		context.Background(), &pb.GetMetadataRequest{
			Topics: []string{"t2"},
		},
	)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	assert.Equal(t, "t2", resp.Topics[0].Name)
}

// --- Subscribe / Poll / Unsubscribe ---

func TestGRPCSubscribePollUnsubscribe(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	_, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "poll-test",
			Partitions: 2,
		},
	)
	require.NoError(t, err)

	subResp, err := client.Subscribe(
		context.Background(), &pb.SubscribeRequest{
			Topic: "poll-test",
			Group: "grp1",
		},
	)
	require.NoError(t, err)
	assert.Empty(t, subResp.Error)
	assert.NotEmpty(t, subResp.MemberId)
	assert.Len(t, subResp.Partitions, 2)

	for i := 0; i < 4; i++ {
		_, err := client.Produce(
			context.Background(), &pb.ProduceRequest{
				Topic: "poll-test",
				Records: []*pb.ProduceRecord{
					{Partition: int32(i % 2), Value: []byte(fmt.Sprintf(
						"msg-%d", i,
					))},
				},
			},
		)
		require.NoError(t, err)
	}

	pollResp, err := client.Poll(
		context.Background(), &pb.PollRequest{
			Topic:      "poll-test",
			Group:      "grp1",
			MemberId:   subResp.MemberId,
			MaxRecords: 100,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, pollResp.Error)
	assert.False(t, pollResp.Rebalance)
	assert.Len(t, pollResp.Messages, 4)

	unsubResp, err := client.Unsubscribe(
		context.Background(), &pb.UnsubscribeRequest{
			Topic:    "poll-test",
			Group:    "grp1",
			MemberId: subResp.MemberId,
		},
	)
	require.NoError(t, err)
	assert.True(t, unsubResp.Success)
}

func TestGRPCPollRebalance(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	_, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       "rebal",
			Partitions: 4,
		},
	)
	require.NoError(t, err)

	// first member subscribes
	sub1, err := client.Subscribe(
		context.Background(), &pb.SubscribeRequest{
			Topic: "rebal",
			Group: "grp",
		},
	)
	require.NoError(t, err)
	assert.Len(t, sub1.Partitions, 4)

	// second member subscribes (triggers rebalance)
	sub2, err := client.Subscribe(
		context.Background(), &pb.SubscribeRequest{
			Topic: "rebal",
			Group: "grp",
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, sub2.MemberId)

	// first member polls - should see rebalance
	pollResp, err := client.Poll(
		context.Background(), &pb.PollRequest{
			Topic:      "rebal",
			Group:      "grp",
			MemberId:   sub1.MemberId,
			MaxRecords: 10,
		},
	)
	require.NoError(t, err)
	assert.True(t, pollResp.Rebalance)
}

// --- Replicate ---

func TestGRPCReplicate(t *testing.T) {
	addr := startTestServer(t)
	client := dialServer(t, addr)

	resp, err := client.Replicate(
		context.Background(), &pb.ReplicateRequest{
			Topic:     "rep-test",
			Partition: 0,
			Records: []*pb.WalRecord{
				{Offset: 0, Key: []byte("k1"), Value: []byte("v1"), Timestamp: 1},
				{Offset: 1, Key: []byte("k2"), Value: []byte("v2"), Timestamp: 2},
			},
		},
	)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Verify we can read back the replicated data.
	consumeResp, err := client.Consume(
		context.Background(), &pb.ConsumeRequest{
			Topic:      "rep-test",
			Partition:  0,
			Offset:     0,
			MaxRecords: 10,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, consumeResp.Error)
	require.Len(t, consumeResp.Messages, 2)
	assert.Equal(t, []byte("v1"), consumeResp.Messages[0].Value)
	assert.Equal(t, []byte("v2"), consumeResp.Messages[1].Value)
}

// --- Context / Timeout ---

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
			Records: []*pb.ProduceRecord{
				{Partition: 0, Value: []byte("v")},
			},
		},
	)
	assert.Error(t, err)
}
