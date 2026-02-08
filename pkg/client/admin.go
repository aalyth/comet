package client

import (
	"context"
	"fmt"
	"log"

	pb "github.com/aalyth/comet/proto/comet/v1"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TopicInfo struct {
	Name       string
	Partitions int32
}

func CreateTopic(brokerAddress, name string, partitions int32) error {
	conn, err := grpc.NewClient(
		brokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return errors.Wrap(err, "connecting")
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Failed closing gRPC connection when creating topic: %v", err)
		}
	}()

	client := pb.NewBrokerServiceClient(conn)
	resp, err := client.CreateTopic(
		context.Background(), &pb.CreateTopicRequest{
			Name:       name,
			Partitions: partitions,
		},
	)
	if err != nil {
		return errors.Wrap(err, "creating topic")
	}
	if resp.Error != "" {
		return fmt.Errorf("creating topic: %s", resp.Error)
	}
	return nil
}

func DeleteTopic(brokerAddress, name string) error {
	conn, err := grpc.NewClient(
		brokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return errors.Wrap(err, "connecting")
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Failed closing gRPC connection when deleting topic: %v", err)
		}
	}()

	client := pb.NewBrokerServiceClient(conn)
	resp, err := client.DeleteTopic(
		context.Background(), &pb.DeleteTopicRequest{
			Name: name,
		},
	)
	if err != nil {
		return errors.Wrap(err, "deleting topic")
	}
	if resp.Error != "" {
		return fmt.Errorf("deleting topic: %s", resp.Error)
	}
	return nil
}

func ListTopics(brokerAddress string) ([]TopicInfo, error) {
	conn, err := grpc.NewClient(
		brokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, errors.Wrap(err, "connecting")
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Failed closing gRPC connection when listing topics: %v", err)
		}
	}()

	client := pb.NewBrokerServiceClient(conn)
	resp, err := client.ListTopics(context.Background(), &pb.ListTopicsRequest{})
	if err != nil {
		return nil, errors.Wrap(err, "listing topics")
	}

	topics := make([]TopicInfo, len(resp.Topics))
	for i, t := range resp.Topics {
		topics[i] = TopicInfo{
			Name:       t.Name,
			Partitions: t.Partitions,
		}
	}
	return topics, nil
}
