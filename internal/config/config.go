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
)

type Config struct {
	DataDir            string
	SegmentMaxBytes    int64
	SegmentMaxAge      time.Duration
	ServerAddress      string
	IndexIntervalBytes int64
}

func Default() *Config {
	return &Config{
		DataDir:            DefaultDirectory,
		SegmentMaxBytes:    DefaultSegmentMaxBytes,
		SegmentMaxAge:      DefaultSegmentMaxAge,
		ServerAddress:      DefaultAddress,
		IndexIntervalBytes: DefaultIndexIntervalBytes,
	}
}
