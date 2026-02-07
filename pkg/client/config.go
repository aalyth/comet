package client

import "time"

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
)

type ProducerConfig struct {
	BrokerAddress string
	BufferSize    int
	FlushInterval time.Duration
	BatchSize     int
}

func DefaultProducerConfig(address string) ProducerConfig {
	return ProducerConfig{
		BrokerAddress: address,
		BufferSize:    DefaultBufferSize,
		FlushInterval: DefaultFlushInterval,
		BatchSize:     DefaultBatchSize,
	}
}

type ConsumerConfig struct {
	BrokerAddress     string
	PollInterval      time.Duration
	MaxPollRecords    int32
	ChannelBuffer     int
	InitialOffset     int64
	BackoffMin        time.Duration
	BackoffMax        time.Duration
	BackoffMultiplier float64
}

func DefaultConsumerConfig(address string) ConsumerConfig {
	return ConsumerConfig{
		BrokerAddress:     address,
		PollInterval:      DefaultPollInterval,
		MaxPollRecords:    DefaultMaxPollRecords,
		ChannelBuffer:     DefaultChannelBuffer,
		InitialOffset:     DefaultInitialOffset,
		BackoffMin:        DefaultBackoffMin,
		BackoffMax:        DefaultBackoffMax,
		BackoffMultiplier: DefaultBackoffMultiplier,
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
