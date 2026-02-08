package config

import "time"

const (
	_ = 1 << (iota * 10)
	KB
	_
	GB
)

const (
	DefaultDirectory          = "data"
	DefaultSegmentMaxBytes    = 1 * GB
	DefaultSegmentMaxAge      = 24 * time.Hour
	DefaultAddress            = ":6174"
	DefaultIndexIntervalBytes = 4 * KB

	DefaultEtcdEndpoint         = "localhost:2379"
	DefaultOffsetCommitCount    = 100
	DefaultOffsetCommitInterval = 500 * time.Millisecond
	DefaultPollTimeout          = 10 * time.Second
)

type Config struct {
	DataDir            string
	SegmentMaxBytes    int64
	SegmentMaxAge      time.Duration
	ServerAddress      string
	IndexIntervalBytes int64

	EtcdEndpoints        []string
	OffsetCommitCount    int
	OffsetCommitInterval time.Duration

	BrokerID         string
	AdvertiseAddress string
	PollTimeout      time.Duration
}

func Default() *Config {
	return &Config{
		DataDir:              DefaultDirectory,
		SegmentMaxBytes:      DefaultSegmentMaxBytes,
		SegmentMaxAge:        DefaultSegmentMaxAge,
		ServerAddress:        DefaultAddress,
		IndexIntervalBytes:   DefaultIndexIntervalBytes,
		EtcdEndpoints:        []string{DefaultEtcdEndpoint},
		OffsetCommitCount:    DefaultOffsetCommitCount,
		OffsetCommitInterval: DefaultOffsetCommitInterval,
		PollTimeout:          DefaultPollTimeout,
	}
}
