package client

import (
	"context"
	"fmt"
	"hash/crc32"
	"net"
	"strings"
	"sync"
	"time"

	pb "github.com/aalyth/comet/proto/comet/v1"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type pendingMessage struct {
	key   []byte
	value []byte
}

// partitionMeta caches which broker leads each partition.
type partitionMeta struct {
	leaderBrokerID string
	leaderAddress  string
}

// TopicProducer is a buffered, async producer bound to a single topic. It
// discovers partition leaders via GetMetadata and routes produce requests
// to the correct broker. Metadata is refreshed periodically and on errors.
type TopicProducer struct {
	config ProducerConfig
	logger *zap.Logger

	// Bootstrap connection (used for metadata + topic creation)
	bootstrapConn   *grpc.ClientConn
	bootstrapClient pb.BrokerServiceClient

	// Per-broker connections for producing
	connectionsMu sync.RWMutex
	connections   map[string]*grpc.ClientConn
	clients       map[string]pb.BrokerServiceClient

	// Partition metadata cache
	metaMu         sync.RWMutex
	partitions     map[int32]*partitionMeta
	partitionCount int32

	mu     sync.Mutex
	buffer []pendingMessage

	roundRobinMu sync.Mutex
	roundRobin   int32

	flushCh  chan struct{}
	doneCh   chan struct{}
	metaDone chan struct{}
	closeCtx context.Context
	cancel   context.CancelFunc
}

func NewTopicProducer(config ProducerConfig) (*TopicProducer, error) {
	logger := config.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	l := logger.Named("producer")

	addrs := config.BootstrapAddresses
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no broker addresses configured")
	}

	// connect to the first reachable bootstrap broker
	var bootstrapConn *grpc.ClientConn
	var bootstrapClient pb.BrokerServiceClient
	var connErr error
	for _, addr := range addrs {
		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			connErr = err
			continue
		}
		bootstrapConn = conn
		bootstrapClient = pb.NewBrokerServiceClient(conn)
		break
	}
	if bootstrapConn == nil {
		return nil, errors.Wrap(connErr, "connecting to bootstrap broker")
	}

	rpc := bootstrapClient

	// Auto-create topic. Retry with backoff because in a multi-broker
	// deployment not all brokers may have registered yet at startup.
	rf := config.ReplicationFactor
	if rf <= 0 {
		rf = 1
	}

	const maxCreateRetries = 10
	backoff := 500 * time.Millisecond
	for attempt := 1; ; attempt++ {
		resp, err := rpc.CreateTopic(
			context.Background(), &pb.CreateTopicRequest{
				Name:              config.Topic,
				Partitions:        config.Partitions,
				ReplicationFactor: rf,
			},
		)
		if err != nil {
			bootstrapConn.Close()
			return nil, errors.Wrap(err, "creating topic")
		}
		if resp.Error == "" || strings.Contains(resp.Error, "already exists") {
			break
		}
		if attempt >= maxCreateRetries {
			bootstrapConn.Close()
			return nil, fmt.Errorf("creating topic: %s", resp.Error)
		}
		l.Info(
			"topic creation not ready, retrying",
			zap.String("error", resp.Error),
			zap.Int("attempt", attempt),
			zap.Duration("backoff", backoff),
		)
		time.Sleep(backoff)
		backoff = time.Duration(float64(backoff) * 1.5)
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &TopicProducer{
		config:          config,
		logger:          l,
		bootstrapConn:   bootstrapConn,
		bootstrapClient: bootstrapClient,
		connections:     make(map[string]*grpc.ClientConn),
		clients:         make(map[string]pb.BrokerServiceClient),
		partitions:      make(map[int32]*partitionMeta),
		buffer:          make([]pendingMessage, 0, config.BufferSize),
		flushCh:         make(chan struct{}, 1),
		doneCh:          make(chan struct{}),
		metaDone:        make(chan struct{}),
		closeCtx:        ctx,
		cancel:          cancel,
	}

	// initial metadata fetch
	if err := p.refreshMetadata(); err != nil {
		l.Warn("initial metadata fetch failed, will retry", zap.Error(err))
	}

	go p.flusher()
	go p.metadataRefresher()

	l.Info(
		"Topic producer created",
		zap.String("topic", config.Topic),
		zap.Int32("partitions", config.Partitions),
	)

	return p, nil
}

func (p *TopicProducer) Send(key, value []byte) error {
	p.mu.Lock()
	if p.closeCtx.Err() != nil {
		p.mu.Unlock()
		return fmt.Errorf("producer is closed")
	}
	p.buffer = append(p.buffer, pendingMessage{key: key, value: value})
	full := len(p.buffer) >= p.config.BufferSize
	p.mu.Unlock()

	if full {
		p.triggerFlush()
	}

	return nil
}

func (p *TopicProducer) Flush() error {
	p.mu.Lock()
	if len(p.buffer) == 0 {
		p.mu.Unlock()
		return nil
	}
	batch := p.buffer
	p.buffer = make([]pendingMessage, 0, p.config.BufferSize)
	p.mu.Unlock()

	// group messages by partition, then by leader broker
	type brokerBatch struct {
		address string
		records []*pb.ProduceRecord
	}
	byBroker := make(map[string]*brokerBatch)

	p.metaMu.RLock()
	partitionCount := p.partitionCount
	partitions := p.partitions
	p.metaMu.RUnlock()

	if partitionCount == 0 {
		partitionCount = p.config.Partitions
	}

	for _, msg := range batch {
		partition := p.selectPartition(msg.key, partitionCount)

		meta, ok := partitions[partition]
		if !ok {
			// use bootstrap broker as fallback
			meta = &partitionMeta{
				leaderAddress: p.config.BootstrapAddresses[0],
			}
		}

		bb, ok := byBroker[meta.leaderAddress]
		if !ok {
			bb = &brokerBatch{address: meta.leaderAddress}
			byBroker[meta.leaderAddress] = bb
		}
		bb.records = append(
			bb.records, &pb.ProduceRecord{
				Partition: partition,
				Key:       msg.key,
				Value:     msg.value,
			},
		)
	}

	var firstErr error
	for _, bb := range byBroker {
		client, err := p.getOrCreateClient(bb.address)
		if err != nil {
			if firstErr == nil {
				firstErr = errors.Wrap(err, "connecting to broker")
			}
			continue
		}

		// send in sub-batches
		for i := 0; i < len(bb.records); i += p.config.BatchSize {
			end := i + p.config.BatchSize
			if end > len(bb.records) {
				end = len(bb.records)
			}

			resp, err := client.Produce(
				context.Background(), &pb.ProduceRequest{
					Topic:   p.config.Topic,
					Records: bb.records[i:end],
				},
			)
			if err != nil {
				if firstErr == nil {
					firstErr = errors.Wrap(err, "producing")
				}
				// purge the stale connection and refresh metadata so
				// the next flush targets the new leader
				p.removeClient(bb.address)
				p.refreshMetadata()
				continue
			}
			if resp.Error != "" {
				if resp.LeaderBrokerId != "" {
					// not leader, so refresh metadata
					p.refreshMetadata()
				}
				if firstErr == nil {
					firstErr = fmt.Errorf("producing: %s", resp.Error)
				}
			}
		}
	}

	if firstErr != nil {
		p.logger.Warn("Error while flushing", zap.Error(firstErr))
	}

	return firstErr
}

func (p *TopicProducer) Close() error {
	p.logger.Info("Closing producer", zap.String("topic", p.config.Topic))
	p.cancel()
	<-p.doneCh
	<-p.metaDone

	if err := p.Flush(); err != nil {
		p.closeAllConns()
		return err
	}

	p.closeAllConns()
	return nil
}

// reconnectBootstrap cycles through the configured bootstrap addresses and
// replaces the current bootstrap connection with the first one that succeeds.
func (p *TopicProducer) reconnectBootstrap() error {
	p.connectionsMu.Lock()
	defer p.connectionsMu.Unlock()

	for _, addr := range p.config.BootstrapAddresses {
		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			continue
		}

		client := pb.NewBrokerServiceClient(conn)

		// verify the connection is actually usable with a metadata call
		_, err = client.GetMetadata(
			context.Background(),
			&pb.GetMetadataRequest{},
		)
		if err != nil {
			conn.Close()
			continue
		}

		// success -- swap out the old connection
		if p.bootstrapConn != nil {
			p.bootstrapConn.Close()
		}
		p.bootstrapConn = conn
		p.bootstrapClient = client

		p.logger.Info("reconnected bootstrap", zap.String("addr", addr))
		return nil
	}

	return fmt.Errorf("no reachable bootstrap broker")
}

func (p *TopicProducer) refreshMetadata() error {
	resp, err := p.bootstrapClient.GetMetadata(
		context.Background(), &pb.GetMetadataRequest{
			Topics: []string{p.config.Topic},
		},
	)
	if err != nil {
		// The current bootstrap broker may be dead. Try reconnecting to
		// another one and retry the metadata fetch once.
		p.logger.Debug(
			"metadata fetch failed, attempting bootstrap reconnect", zap.Error(err),
		)
		if reconnErr := p.reconnectBootstrap(); reconnErr != nil {
			return errors.Wrap(err, "fetching metadata")
		}
		resp, err = p.bootstrapClient.GetMetadata(
			context.Background(), &pb.GetMetadataRequest{
				Topics: []string{p.config.Topic},
			},
		)
		if err != nil {
			return errors.Wrap(err, "fetching metadata after reconnect")
		}
	}

	// Build broker ID -> address map. Normalize addresses that are just a
	// port (e.g. ":6174") by using the host from the bootstrap address.
	bootstrapHost := extractHost(p.config.BootstrapAddresses[0])
	brokerAddrs := make(map[string]string)
	for _, b := range resp.Brokers {
		brokerAddrs[b.Id] = normalizeAddr(b.Address, bootstrapHost)
	}

	// update partition metadata
	p.metaMu.Lock()
	defer p.metaMu.Unlock()

	for _, topic := range resp.Topics {
		if topic.Name != p.config.Topic {
			continue
		}
		p.partitionCount = int32(len(topic.Partitions))
		for _, pm := range topic.Partitions {
			addr, ok := brokerAddrs[pm.LeaderBrokerId]
			if !ok {
				continue
			}
			p.partitions[pm.Id] = &partitionMeta{
				leaderBrokerID: pm.LeaderBrokerId,
				leaderAddress:  addr,
			}
		}
	}

	p.logger.Debug("metadata refreshed", zap.Int32("partitions", p.partitionCount))
	return nil
}

func (p *TopicProducer) selectPartition(key []byte, partitionCount int32) int32 {
	if partitionCount <= 0 {
		return 0
	}
	if len(key) > 0 {
		return int32(crc32.ChecksumIEEE(key) % uint32(partitionCount))
	}

	p.roundRobinMu.Lock()
	defer p.roundRobinMu.Unlock()
	partition := p.roundRobin
	p.roundRobin = (partition + 1) % partitionCount
	return partition
}

func (p *TopicProducer) getOrCreateClient(address string) (pb.BrokerServiceClient, error) {
	p.connectionsMu.RLock()
	client, ok := p.clients[address]
	p.connectionsMu.RUnlock()
	if ok {
		return client, nil
	}

	p.connectionsMu.Lock()
	defer p.connectionsMu.Unlock()

	if client, ok := p.clients[address]; ok {
		return client, nil
	}

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	client = pb.NewBrokerServiceClient(conn)
	p.connections[address] = conn
	p.clients[address] = client
	return client, nil
}

// removeClient closes and removes a cached broker connection so the next
// flush creates a fresh one (after metadata has been refreshed).
func (p *TopicProducer) removeClient(address string) {
	p.connectionsMu.Lock()
	defer p.connectionsMu.Unlock()

	if conn, ok := p.connections[address]; ok {
		conn.Close()
		delete(p.connections, address)
		delete(p.clients, address)
	}
}

func (p *TopicProducer) closeAllConns() {
	p.connectionsMu.Lock()
	defer p.connectionsMu.Unlock()

	for _, conn := range p.connections {
		conn.Close()
	}
	p.connections = make(map[string]*grpc.ClientConn)
	p.clients = make(map[string]pb.BrokerServiceClient)

	if p.bootstrapConn != nil {
		p.bootstrapConn.Close()
	}
}

func (p *TopicProducer) triggerFlush() {
	select {
	case p.flushCh <- struct{}{}:
	default:
	}
}

func (p *TopicProducer) flusher() {
	defer close(p.doneCh)

	ticker := time.NewTicker(p.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.Flush()
		case <-p.flushCh:
			p.Flush()
		case <-p.closeCtx.Done():
			return
		}
	}
}

func (p *TopicProducer) metadataRefresher() {
	defer close(p.metaDone)

	interval := p.config.MetadataRefreshInterval
	if interval <= 0 {
		interval = DefaultMetadataRefreshInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := p.refreshMetadata(); err != nil {
				p.logger.Debug("metadata refresh failed", zap.Error(err))
			}
		case <-p.closeCtx.Done():
			return
		}
	}
}

// extractHost returns the host portion of an address. For "localhost:6174" it
// returns "localhost". For ":6174" it returns "localhost".
func extractHost(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" {
		return "localhost"
	}
	return host
}

// normalizeAddr fixes addresses that are just a port (":6174") by prepending
// the given fallback host, so they become "localhost:6174".
func normalizeAddr(addr, fallbackHost string) string {
	if strings.HasPrefix(addr, ":") {
		return fallbackHost + addr
	}
	host, _, err := net.SplitHostPort(addr)
	if err == nil && host == "" {
		return fallbackHost + addr
	}
	return addr
}
