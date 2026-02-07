package server

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/aalyth/comet/internal/broker"
	"github.com/aalyth/comet/internal/config"
	pb "github.com/aalyth/comet/proto/comet/v1"
	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedBrokerServiceServer
	broker     *broker.Broker
	grpcServer *grpc.Server
	config     *config.Config
}

func New(cfg *config.Config) (*Server, error) {
	brk, err := broker.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("create broker: %w", err)
	}

	return &Server{
		broker:     brk,
		grpcServer: grpc.NewServer(),
		config:     cfg,
	}, nil
}

func (s *Server) Start() error {
	pb.RegisterBrokerServiceServer(s.grpcServer, s)

	listener, err := net.Listen("tcp", s.config.ServerAddress)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	fmt.Printf("Server listening on %s\n", s.config.ServerAddress)
	return s.grpcServer.Serve(listener)
}

func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
	s.broker.Close()
}

func runWithContext[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	type result struct {
		value T
		err   error
	}

	resultCh := make(chan result, 1)

	go func() {
		value, err := fn()
		resultCh <- result{value: value, err: err}
	}()

	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case res := <-resultCh:
		return res.value, res.err
	}
}

func (s *Server) CreateTopic(
	ctx context.Context,
	req *pb.CreateTopicRequest,
) (*pb.CreateTopicResponse, error) {
	_, err := runWithContext(
		ctx, func() (struct{}, error) {
			return struct{}{}, s.broker.CreateTopic(req.Name, req.Partitions)
		},
	)

	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return &pb.CreateTopicResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.CreateTopicResponse{
		Success: true,
	}, nil
}

func (s *Server) DeleteTopic(
	ctx context.Context,
	req *pb.DeleteTopicRequest,
) (*pb.DeleteTopicResponse, error) {
	_, err := runWithContext(
		ctx, func() (struct{}, error) {
			return struct{}{}, s.broker.DeleteTopic(req.Name)
		},
	)

	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return &pb.DeleteTopicResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.DeleteTopicResponse{
		Success: true,
	}, nil
}

func (s *Server) ListTopics(
	ctx context.Context,
	_ *pb.ListTopicsRequest,
) (*pb.ListTopicsResponse, error) {
	topics, err := runWithContext(
		ctx, func() ([]*broker.TopicInfo, error) {
			return s.broker.ListTopics()
		},
	)

	if err != nil {
		return nil, err
	}

	topicInfos := make([]*pb.TopicInfo, len(topics))
	for i, topic := range topics {
		topicInfos[i] = &pb.TopicInfo{
			Name:       topic.Name,
			Partitions: topic.Partitions,
		}
	}

	return &pb.ListTopicsResponse{
		Topics: topicInfos,
	}, nil
}

type produceResult struct {
	partition int32
	offset    int64
}

func (s *Server) Produce(ctx context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	res, err := runWithContext(
		ctx, func() (produceResult, error) {
			partition, offset, err := s.broker.Produce(req.Topic, req.Key, req.Value)
			return produceResult{partition: partition, offset: offset}, err
		},
	)

	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return &pb.ProduceResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.ProduceResponse{
		Offset: res.offset,
	}, nil
}

func (s *Server) Consume(req *pb.ConsumeRequest, stream pb.BrokerService_ConsumeServer) error {
	ctx := stream.Context()
	const batchSize = 100

	records, err := runWithContext(
		ctx,
		func() ([]*pb.WalRecord, error) {
			return s.broker.Consume(req.Topic, req.Partition, req.Offset, batchSize)
		},
	)

	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return stream.Send(&pb.ConsumeResponse{Error: err.Error()})
	}

	for _, record := range records {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg := &pb.Message{
			Offset:    record.Offset,
			Key:       record.Key,
			Value:     record.Value,
			Timestamp: record.Timestamp,
		}

		if err := stream.Send(&pb.ConsumeResponse{Message: msg}); err != nil {
			return err
		}
	}

	return nil
}
