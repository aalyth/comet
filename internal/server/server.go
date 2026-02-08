package server

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/aalyth/comet/internal/broker"
	"github.com/aalyth/comet/internal/config"
	pb "github.com/aalyth/comet/proto/comet/v1"
	pkgerr "github.com/pkg/errors"
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
		return nil, pkgerr.Wrap(err, "creating broker")
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
		return pkgerr.Wrap(err, "listening to TCP")
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

func (s *Server) Broker() *broker.Broker {
	return s.broker
}

func (s *Server) CreateTopic(
	ctx context.Context,
	req *pb.CreateTopicRequest,
) (*pb.CreateTopicResponse, error) {
	s.logger.Info(
		"CreateTopic",
		zap.String("topic", req.Name),
		zap.Int32("partitions", req.Partitions),
		zap.Int32("replicationFactor", req.ReplicationFactor),
	)

	rf := req.ReplicationFactor
	if rf <= 0 {
		rf = 1
	}

	if err := s.broker.CreateTopic(req.Name, req.Partitions, rf); err != nil {
		s.logger.Warn(
			"Failed creating topic", zap.String("topic", req.Name), zap.Error(err),
		)
		return &pb.CreateTopicResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.CreateTopicResponse{Success: true}, nil
}

func (s *Server) DeleteTopic(
	ctx context.Context,
	req *pb.DeleteTopicRequest,
) (*pb.DeleteTopicResponse, error) {
	s.logger.Info("DeleteTopic", zap.String("topic", req.Name))

	if err := s.broker.DeleteTopic(req.Name); err != nil {
		s.logger.Warn(
			"Failed deleting topic", zap.String("topic", req.Name), zap.Error(err),
		)
		return &pb.DeleteTopicResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.DeleteTopicResponse{Success: true}, nil
}

func (s *Server) ListTopics(
	ctx context.Context,
	_ *pb.ListTopicsRequest,
) (*pb.ListTopicsResponse, error) {
	topics, err := s.broker.ListTopics()
	if err != nil {
		return nil, err
	}

	infos := make([]*pb.TopicInfo, len(topics))
	for i, t := range topics {
		infos[i] = &pb.TopicInfo{
			Name:              t.Name,
			Partitions:        t.Partitions,
			ReplicationFactor: t.ReplicationFactor,
		}
	}

	return &pb.ListTopicsResponse{Topics: infos}, nil
}

func (s *Server) GetMetadata(
	ctx context.Context,
	req *pb.GetMetadataRequest,
) (*pb.GetMetadataResponse, error) {
	liveBrokers := s.broker.Registry().LiveBrokers()
	brokerInfos := make([]*pb.BrokerInfo, 0, len(liveBrokers))
	for id, addr := range liveBrokers {
		brokerInfos = append(brokerInfos, &pb.BrokerInfo{Id: id, Address: addr})
	}

	allAssignments := s.broker.Assignments().AllAssignments()
	allMeta := s.broker.Assignments().AllTopicsMeta()

	requested := make(map[string]bool)
	for _, t := range req.Topics {
		requested[t] = true
	}

	topicMetas := make([]*pb.TopicMetadata, 0)
	for topic, assignment := range allAssignments {
		if len(requested) > 0 && !requested[topic] {
			continue
		}

		partMetas := make([]*pb.PartitionMetadata, 0, len(assignment.Partitions))
		for pid, pa := range assignment.Partitions {
			partMetas = append(
				partMetas, &pb.PartitionMetadata{
					Id:               pid,
					LeaderBrokerId:   pa.LeaderID,
					ReplicaBrokerIds: pa.ReplicaIDs,
				},
			)
		}

		rf := assignment.ReplicationFactor
		if meta, ok := allMeta[topic]; ok {
			rf = meta.ReplicationFactor
		}

		topicMetas = append(
			topicMetas, &pb.TopicMetadata{
				Name:              topic,
				Partitions:        partMetas,
				ReplicationFactor: rf,
			},
		)
	}

	return &pb.GetMetadataResponse{
		Brokers: brokerInfos,
		Topics:  topicMetas,
	}, nil
}

func (s *Server) Produce(
	ctx context.Context,
	req *pb.ProduceRequest,
) (*pb.ProduceResponse, error) {
	results, err := s.broker.Produce(req.Topic, req.Records)
	if err != nil {
		var notLeader *broker.ErrNotLeader
		if errors.As(err, &notLeader) {
			return &pb.ProduceResponse{
				Error:          notLeader.Error(),
				LeaderBrokerId: notLeader.LeaderID,
			}, nil
		}

		s.logger.Warn("Failed producing", zap.String("topic", req.Topic), zap.Error(err))
		return &pb.ProduceResponse{Error: err.Error()}, nil
	}

	return &pb.ProduceResponse{Results: results}, nil
}

func (s *Server) Consume(
	ctx context.Context,
	req *pb.ConsumeRequest,
) (*pb.ConsumeResponse, error) {
	maxRecords := req.MaxRecords
	if maxRecords <= 0 {
		maxRecords = 100
	}

	records, err := s.broker.Consume(req.Topic, req.Partition, req.Offset, int(maxRecords))
	if err != nil {
		return &pb.ConsumeResponse{Error: err.Error()}, nil
	}

	msgs := make([]*pb.Message, len(records))
	for i, r := range records {
		msgs[i] = &pb.Message{
			Offset:    r.Offset,
			Key:       r.Key,
			Value:     r.Value,
			Timestamp: r.Timestamp,
			Partition: req.Partition,
			Topic:     req.Topic,
		}
	}

	return &pb.ConsumeResponse{Messages: msgs}, nil
}

func (s *Server) Subscribe(
	ctx context.Context,
	req *pb.SubscribeRequest,
) (*pb.SubscribeResponse, error) {
	s.logger.Info(
		"Subscribe",
		zap.String("topic", req.Topic),
		zap.String("group", req.Group),
	)

	member, generation, err := s.broker.JoinGroup(req.Topic, req.Group)
	if err != nil {
		s.logger.Warn("Failed joining group", zap.Error(err))
		return &pb.SubscribeResponse{Error: err.Error()}, nil
	}

	s.logger.Info(
		"Member joined group",
		zap.String("memberID", member.ID),
		zap.Int32s("partitions", member.Partitions()),
		zap.Int64("generation", generation),
	)

	return &pb.SubscribeResponse{
		MemberId:   member.ID,
		Partitions: member.Partitions(),
		Generation: generation,
	}, nil
}

func (s *Server) Poll(
	ctx context.Context,
	req *pb.PollRequest,
) (*pb.PollResponse, error) {
	partitions, rebalance, generation, err := s.broker.PollGroup(
		req.Topic, req.Group, req.MemberId,
	)
	if err != nil {
		return &pb.PollResponse{Error: err.Error()}, nil
	}

	// if rebalance occurred, tell the client to re-subscribe
	if rebalance {
		return &pb.PollResponse{
			Rebalance:  true,
			Generation: generation,
		}, nil
	}

	// read from each assigned partition
	maxRecords := req.MaxRecords
	if maxRecords <= 0 {
		maxRecords = 100
	}

	perPartition := maxRecords
	if len(partitions) > 0 {
		perPartition = maxRecords / int32(len(partitions))
		if perPartition <= 0 {
			perPartition = 1
		}
	}

	var allMsgs []*pb.Message
	for _, partition := range partitions {
		offset := s.broker.GetMemberOffset(req.MemberId, partition)

		msgs, err := s.broker.ConsumeOrProxy(req.Topic, partition, offset, perPartition)
		if err != nil {
			s.logger.Debug(
				"Consume error during poll",
				zap.Int32("partition", partition),
				zap.Error(err),
			)
			continue
		}

		for _, msg := range msgs {
			allMsgs = append(allMsgs, msg)
			newOffset := msg.Offset + 1
			s.broker.SetMemberOffset(req.MemberId, partition, newOffset)
			s.broker.CommitOffset(req.Topic, req.Group, partition, newOffset)
		}
	}

	return &pb.PollResponse{
		Messages:   allMsgs,
		Generation: generation,
	}, nil
}

func (s *Server) Unsubscribe(
	ctx context.Context,
	req *pb.UnsubscribeRequest,
) (*pb.UnsubscribeResponse, error) {
	s.logger.Info(
		"Unsubscribe",
		zap.String("topic", req.Topic),
		zap.String("group", req.Group),
		zap.String("memberID", req.MemberId),
	)

	if err := s.broker.LeaveGroup(req.Topic, req.Group, req.MemberId); err != nil {
		return &pb.UnsubscribeResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.UnsubscribeResponse{Success: true}, nil
}

func (s *Server) Replicate(
	ctx context.Context,
	req *pb.ReplicateRequest,
) (*pb.ReplicateResponse, error) {
	s.logger.Debug(
		"Replicate",
		zap.String("topic", req.Topic),
		zap.Int32("partition", req.Partition),
		zap.Int("records", len(req.Records)),
	)

	// ensure the local partition exists
	stor := s.broker.Storage()
	if err := stor.EnsurePartition(req.Topic, req.Partition); err != nil {
		return &pb.ReplicateResponse{
			Success: false,
			Error:   fmt.Sprintf("ensuring partition: %v", err),
		}, nil
	}

	t, err := stor.GetTopic(req.Topic)
	if err != nil {
		return &pb.ReplicateResponse{
			Success: false,
			Error:   fmt.Sprintf("getting topic: %v", err),
		}, nil
	}

	p, err := t.GetPartition(req.Partition)
	if err != nil {
		return &pb.ReplicateResponse{
			Success: false,
			Error:   fmt.Sprintf("getting partition: %v", err),
		}, nil
	}

	// write each record to local storage
	for _, record := range req.Records {
		if _, err := p.Append(record.Key, record.Value); err != nil {
			return &pb.ReplicateResponse{
				Success: false,
				Error:   fmt.Sprintf("appending record: %v", err),
			}, nil
		}
	}

	return &pb.ReplicateResponse{Success: true}, nil
}
