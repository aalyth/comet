package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/aalyth/comet/internal/broker"
	"github.com/aalyth/comet/internal/config"
	pb "github.com/aalyth/comet/proto/comet/v1"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedBrokerServiceServer
	broker     *broker.Broker
	grpcServer *grpc.Server
	config     *config.Config
	logger     *zap.Logger
}

func New(cfg *config.Config, logger *zap.Logger) (*Server, error) {
	l := logger.Named("server")

	brk, err := broker.New(cfg, logger)
	if err != nil {
		return nil, errors.Wrap(err, "creating broker")
	}

	return &Server{
		broker:     brk,
		grpcServer: grpc.NewServer(),
		config:     cfg,
		logger:     l,
	}, nil
}

func (s *Server) Start() error {
	pb.RegisterBrokerServiceServer(s.grpcServer, s)

	listener, err := net.Listen("tcp", s.config.ServerAddress)
	if err != nil {
		return errors.Wrap(err, "listening to TCP")
	}

	s.logger.Info("Listening", zap.String("addr", s.config.ServerAddress))
	return s.grpcServer.Serve(listener)
}

func (s *Server) Stop() {
	s.logger.Info("Shutting down")
	s.grpcServer.GracefulStop()
	if err := s.broker.Close(); err != nil {
		s.logger.Error("Failed closing down broker", zap.Error(err))
	}
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
	s.logger.Info(
		"CreateTopic",
		zap.String("topic", req.Name),
		zap.Int32("partitions", req.Partitions),
	)

	_, err := runWithContext(
		ctx,
		func() (struct{}, error) {
			return struct{}{}, s.broker.CreateTopic(req.Name, req.Partitions)
		},
	)

	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}

		s.logger.Warn(
			"Failed creating topic",
			zap.String("topic", req.Name),
			zap.Error(err),
		)
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
	s.logger.Info("Deleting topic", zap.String("topic", req.Name))

	_, err := runWithContext(
		ctx,
		func() (struct{}, error) {
			return struct{}{}, s.broker.DeleteTopic(req.Name)
		},
	)

	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}

		s.logger.Warn(
			"Failed deleting topic",
			zap.String("topic", req.Name),
			zap.Error(err),
		)
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
		ctx,
		func() ([]*broker.TopicInfo, error) {
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

	s.logger.Debug("Listing topics", zap.Int("count", len(topicInfos)))
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
		ctx,
		func() (produceResult, error) {
			partition, offset, err := s.broker.Produce(req.Topic, req.Key, req.Value)
			return produceResult{partition: partition, offset: offset}, err
		},
	)

	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}

		s.logger.Warn(
			"Failed producing to topic",
			zap.String("topic", req.Topic),
			zap.Error(err),
		)
		return &pb.ProduceResponse{
			Error: err.Error(),
		}, nil
	}

	s.logger.Debug(
		"Produced",
		zap.String("topic", req.Topic),
		zap.Int32("partition", res.partition),
		zap.Int64("offset", res.offset),
	)

	return &pb.ProduceResponse{
		Offset: res.offset,
	}, nil
}

func (s *Server) Consume(req *pb.ConsumeRequest, stream pb.BrokerService_ConsumeServer) error {
	ctx := stream.Context()
	const batchSize = 100

	s.logger.Debug(
		"Consuming",
		zap.String("topic", req.Topic),
		zap.Int32("partition", req.Partition),
		zap.Int64("offset", req.Offset),
	)

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

func (s *Server) Subscribe(
	req *pb.SubscribeRequest,
	stream pb.BrokerService_SubscribeServer,
) error {
	ctx := stream.Context()

	s.logger.Info(
		"Subscribing",
		zap.String("topic", req.Topic),
		zap.String("group", req.Group),
	)

	// Look up the topic to get the partition count.
	numPartitions, err := s.broker.GetTopicPartitionCount(req.Topic)
	if err != nil {
		s.logger.Debug(
			"Subscribing to a non-existing topic",
			zap.String("topic", req.Topic),
			zap.Error(err),
		)
		return stream.Send(
			&pb.SubscribeResponse{
				Error: fmt.Sprintf("topic %q not found", req.Topic),
			},
		)
	}

	member, cleanup, err := s.broker.JoinGroup(req.Topic, req.Group, numPartitions)
	if err != nil {
		s.logger.Error(
			"Failed joining group during subscription",
			zap.String("topic", req.Topic),
			zap.String("group", req.Group),
			zap.Error(err),
		)
		return stream.Send(
			&pb.SubscribeResponse{
				Error: fmt.Sprintf("joining group: %v", err),
			},
		)
	}
	defer func() {
		cleanup()
		s.logger.Info(
			"Closing down subscriber stream",
			zap.String("topic", req.Topic),
			zap.String("group", req.Group),
			zap.String("memberID", member.ID),
		)
	}()

	s.logger.Info(
		"Joined consumer group",
		zap.String("topic", req.Topic),
		zap.String("group", req.Group),
		zap.String("memberID", member.ID),
		zap.Int32s("partitions", member.Partitions()),
	)

	offsets := make(map[int32]int64)
	for _, p := range member.Partitions() {
		offset, err := s.broker.GetGroupOffset(req.Topic, req.Group, p)
		if err != nil {
			// default offset `EARLIEST`
			offset = 0
		}
		offsets[p] = offset
	}

	const batchSize = 100
	pollInterval := 50 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// check for rebalance
		select {
		case <-s.broker.WaitRebalance(req.Topic, req.Group):
			newPartitions := member.Partitions()
			s.logger.Info(
				"Detected rebalance during subscription",
				zap.String("memberID", member.ID),
				zap.Int32s("partitions", newPartitions),
			)

			// Partitions changed -- refresh assignments and offsets.
			offsets = make(map[int32]int64)
			for _, p := range newPartitions {
				offset, err := s.broker.GetGroupOffset(req.Topic, req.Group, p)
				if err != nil {
					offset = 0
				}
				offsets[p] = offset
			}
		default:
		}

		partitions := member.Partitions()
		if len(partitions) == 0 {
			// no partitions assigned yet, wait briefly
			select {
			case <-time.After(pollInterval):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		sentAny := false
		for _, partition := range partitions {
			offset := offsets[partition]

			records, err := s.broker.Consume(req.Topic, partition, offset, batchSize)
			if err != nil {
				continue
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
					Partition: partition,
					Topic:     req.Topic,
				}

				if err := stream.Send(&pb.SubscribeResponse{Message: msg}); err != nil {
					return err
				}

				offsets[partition] = record.Offset + 1
				s.broker.SetGroupOffset(
					req.Topic, req.Group, partition, record.Offset+1,
				)
				sentAny = true
			}
		}

		if !sentAny {
			select {
			case <-time.After(pollInterval):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
