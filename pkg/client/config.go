package client

import (
	"strings"
	"time"

	"go.uber.org/zap"
)

// ParseAddresses splits a comma-separated list of broker addresses into a
// slice (e.g. "broker1:6174,broker2:6174" -> ["broker1:6174", "broker2:6174"]).
func ParseAddresses(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if addr := strings.TrimSpace(p); addr != "" {
			result = append(result, addr)
		}
	}
	return result
}

const (
	DefaultBufferSize    = 1000
	DefaultFlushInterval = 100 * time.Millisecond
	DefaultBatchSize     = 100

	DefaultPollInterval      = 500 * time.Millisecond
	DefaultMaxPollRecords    = int32(100)
	DefaultChannelBuffer     = 256
	DefaultInitialOffset     = int64(0)
	DefaultBackoffMin        = 50 * time.Millisecond
	DefaultBackoffMax        = 5 * time.Second
	DefaultBackoffMultiplier = 2.0

	DefaultMetadataRefreshInterval = 30 * time.Second
)

type ProducerConfig struct {
	// BootstrapAddresses is a list of broker addresses used for initial
	// cluster discovery. At least one must be reachable.
	BootstrapAddresses []string

	Topic string

	// Topics get implicitly created when first written to, so we need to
	// have the default partitions count.
	Partitions        int32
	ReplicationFactor int32

	BufferSize    int
	FlushInterval time.Duration
	BatchSize     int

	MetadataRefreshInterval time.Duration

	// optional; nil = silent
	Logger *zap.Logger
}

func DefaultProducerConfig(addresses []string, topic string, partitions int32) ProducerConfig {
	return ProducerConfig{
		BootstrapAddresses:      addresses,
		Topic:                   topic,
		Partitions:              partitions,
		ReplicationFactor:       1,
		BufferSize:              DefaultBufferSize,
		FlushInterval:           DefaultFlushInterval,
		BatchSize:               DefaultBatchSize,
		MetadataRefreshInterval: DefaultMetadataRefreshInterval,
	}
}

type ConsumerConfig struct {
	// BootstrapAddresses is a list of broker addresses used for initial
	// cluster discovery. At least one must be reachable.
	BootstrapAddresses []string

	Group string

	PollInterval   time.Duration
	MaxPollRecords int32
	ChannelBuffer  int
	InitialOffset  int64

	BackoffMin        time.Duration
	BackoffMax        time.Duration
	BackoffMultiplier float64

	// optional; nil = silent
	Logger *zap.Logger
}

func DefaultConsumerConfig(addresses []string, group string) ConsumerConfig {
	return ConsumerConfig{
		BootstrapAddresses: addresses,
		Group:              group,
		PollInterval:       DefaultPollInterval,
		MaxPollRecords:     DefaultMaxPollRecords,
		ChannelBuffer:      DefaultChannelBuffer,
		InitialOffset:      DefaultInitialOffset,
		BackoffMin:         DefaultBackoffMin,
		BackoffMax:         DefaultBackoffMax,
		BackoffMultiplier:  DefaultBackoffMultiplier,
	}
}

type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp int64
}

type MessageHandler = func(msg *Message)
