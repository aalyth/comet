package broker

import (
	"sync"
	"time"

	pb "github.com/aalyth/comet/proto/comet/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PeerManager manages gRPC connections to other brokers. Connections are
// created lazily on first use and cached for reuse.
type PeerManager struct {
	registry *BrokerRegistry

	mu          sync.RWMutex
	connections map[string]*grpc.ClientConn
	clients     map[string]pb.BrokerServiceClient

	logger *zap.Logger
}

func NewPeerManager(registry *BrokerRegistry, logger *zap.Logger) *PeerManager {
	return &PeerManager{
		registry:    registry,
		connections: make(map[string]*grpc.ClientConn),
		clients:     make(map[string]pb.BrokerServiceClient),
		logger:      logger.Named("peers"),
	}
}

func (pm *PeerManager) GetClient(brokerID string) (pb.BrokerServiceClient, error) {
	pm.mu.RLock()
	client, ok := pm.clients[brokerID]
	pm.mu.RUnlock()
	if ok {
		return client, nil
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// double-check after acquiring write lock
	if client, ok := pm.clients[brokerID]; ok {
		return client, nil
	}

	addr, ok := pm.registry.BrokerAddress(brokerID)
	if !ok {
		return nil, &ErrBrokerNotFound{BrokerID: brokerID}
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(
			grpc.ConnectParams{
				MinConnectTimeout: 5 * time.Second,
			},
		),
	)
	if err != nil {
		return nil, err
	}

	client = pb.NewBrokerServiceClient(conn)
	pm.connections[brokerID] = conn
	pm.clients[brokerID] = client

	pm.logger.Debug(
		"Peer connection created",
		zap.String("brokerID", brokerID),
		zap.String("address", addr),
	)
	return client, nil
}

func (pm *PeerManager) RemoveClient(brokerID string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if conn, ok := pm.connections[brokerID]; ok {
		conn.Close()
		delete(pm.connections, brokerID)
		delete(pm.clients, brokerID)
		pm.logger.Debug("peer connection removed", zap.String("brokerID", brokerID))
	}
}

func (pm *PeerManager) Close() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for id, conn := range pm.connections {
		if err := conn.Close(); err != nil {
			pm.logger.Warn(
				"failed closing peer connection",
				zap.String("brokerID", id),
				zap.Error(err),
			)
		}
	}
	pm.connections = make(map[string]*grpc.ClientConn)
	pm.clients = make(map[string]pb.BrokerServiceClient)

	pm.logger.Info("all peer connections closed")
	return nil
}

type ErrBrokerNotFound struct {
	BrokerID string
}

func (e *ErrBrokerNotFound) Error() string {
	return "broker not found: " + e.BrokerID
}
