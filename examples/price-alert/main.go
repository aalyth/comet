package main

import (
	"encoding/json"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/aalyth/comet/pkg/client"
	"go.uber.org/zap"
)

type PriceUpdate struct {
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	Timestamp int64   `json:"timestamp"`
}

const (
	priceAlertThresholdEnvVar  = "THRESHOLD_PCT"
	defaultPriceAlertThreshold = 5.0

	brokerAddressEnvVar  = "BROKER_ADDR"
	defaultBrokerAddress = "localhost:6174"
)

func getPriceAlertThreshold() float64 {
	threshold := defaultPriceAlertThreshold
	if s := os.Getenv(priceAlertThresholdEnvVar); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			threshold = v
		}
	}
	return threshold
}

type PriceAlerter struct {
	mu        sync.Mutex
	baselines map[string]float64
	logger    *zap.Logger
	threshold float64
}

func (pa *PriceAlerter) consumePrice(msg *client.Message) {
	var update PriceUpdate
	if err := json.Unmarshal(msg.Value, &update); err != nil {
		pa.logger.Warn("unmarshal error", zap.Error(err))
		return
	}

	pa.mu.Lock()
	baseline, exists := pa.baselines[update.Symbol]
	if !exists {
		pa.baselines[update.Symbol] = update.Price
		pa.mu.Unlock()
		pa.logger.Info(
			"baseline set",
			zap.String("symbol", update.Symbol),
			zap.Float64("price", update.Price),
		)
		return
	}
	pa.mu.Unlock()

	changePct := ((update.Price - baseline) / baseline) * 100
	if math.Abs(changePct) >= pa.threshold {
		pa.logger.Error(
			"ALERT",
			zap.String("symbol", update.Symbol),
			zap.Float64("change_pct", changePct),
			zap.Float64("baseline", baseline),
			zap.Float64("current", update.Price),
		)
	} else {
		pa.logger.Debug(
			"price tick",
			zap.String("symbol", update.Symbol),
			zap.Float64("price", update.Price),
			zap.Float64("change_pct", changePct),
		)
	}

}

func main() {
	addr := os.Getenv(brokerAddressEnvVar)
	if addr == "" {
		addr = defaultBrokerAddress
	}

	logger, _ := zap.NewProduction()
	defer func() {
		if err := logger.Sync(); err != nil {
			log.Fatalf("Failed flushing zap logger: %v", err)
		}
	}()

	priceAlerter := &PriceAlerter{
		baselines: make(map[string]float64),
		logger:    logger,
		threshold: getPriceAlertThreshold(),
	}

	cfg := client.DefaultConsumerConfig(addr, "price-alerts")
	cfg.Logger = logger
	consumer, err := client.NewConsumer(cfg)
	if err != nil {
		logger.Fatal("failed to create consumer", zap.Error(err))
	}

	if err := consumer.Subscribe("prices", priceAlerter.consumePrice); err != nil {
		logger.Fatal("failed to subscribe", zap.Error(err))
	}

	logger.Info(
		"consumer started",
		zap.String("group", "price-alerts"),
		zap.Float64("threshold", priceAlerter.threshold),
	)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down consumer")
	if err := consumer.Close(); err != nil {
		logger.Error("Failed closing Comet consumer", zap.Error(err))
	}
}
