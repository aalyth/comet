package main

import (
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aalyth/comet/pkg/client"
	"go.uber.org/zap"
)

type PriceUpdate struct {
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	Timestamp int64   `json:"timestamp"`
}

type Ticker struct {
	symbol string
	price  float64
}

const (
	brokerAddressEnvVar  = "BROKER_ADDR"
	defaultBrokerAddress = "localhost:6174"
)

var tickers = []Ticker{
	{"AAPL", 182.50},
	{"GOOG", 141.80},
	{"MSFT", 378.90},
	{"AMZN", 211.70},
	{"TSLA", 411.60},
	{"NVDA", 185.20},
	{"META", 663.90},
	{"AMD", 208.60},
	{"CSCO", 84.90},
	{"ARM", 123.70},
}

func randomTimeMsBetween(fromMs, toMs int) time.Duration {
	if toMs < fromMs {
		log.Fatalf("Invalid random duration - `toMs` must be higher than `fromMs`")

	}

	return time.Duration(fromMs+rand.Intn(toMs-fromMs)) * time.Millisecond
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

	cfg := client.DefaultProducerConfig(addr, "prices", 3)
	cfg.Logger = logger
	producer, err := client.NewTopicProducer(cfg)
	if err != nil {
		logger.Fatal("failed to create producer", zap.Error(err))
	}

	prices := make(map[string]float64)
	for _, t := range tickers {
		prices[t.symbol] = t.price
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("producer started — emitting prices at ~5/sec per ticker")

	for {
		select {
		case <-sigCh:
			logger.Info("shutting down producer")
			if err := producer.Close(); err != nil {
				logger.Fatal("Failed closing Comet producer", zap.Error(err))
			}
			return
		default:
		}

		t := tickers[rand.Intn(len(tickers))]

		// random movement with ~1% standard deviation per tick
		prices[t.symbol] *= 1.0 + rand.NormFloat64()*0.01
		price := math.Round(prices[t.symbol]*100) / 100

		update := PriceUpdate{
			Symbol:    t.symbol,
			Price:     price,
			Timestamp: time.Now().UnixMilli(),
		}
		data, _ := json.Marshal(update)

		if err := producer.Send([]byte(t.symbol), data); err != nil {
			logger.Warn("Failed sending price update", zap.Error(err))
			continue
		}

		logger.Info(
			"PRICE",
			zap.String("symbol", t.symbol),
			zap.Float64("price", price),
		)

		time.Sleep(randomTimeMsBetween(50, 150))
	}
}
