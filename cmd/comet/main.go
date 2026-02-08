package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aalyth/comet/internal/config"
	"github.com/aalyth/comet/internal/logging"
	"github.com/aalyth/comet/internal/server"
	"go.uber.org/zap"
)

func main() {
	cfg := config.Default()

	var etcdEndpoints string
	var logLevel string

	flag.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "Data directory")
	flag.StringVar(&cfg.ServerAddress, "addr", cfg.ServerAddress, "Server address")
	flag.StringVar(
		&etcdEndpoints, "etcd", strings.Join(cfg.EtcdEndpoints, ","),
		"Comma-separated etcd endpoints",
	)
	flag.StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	flag.StringVar(&cfg.BrokerID, "broker-id", "", "Broker ID (auto-generated UUID if empty)")
	flag.StringVar(
		&cfg.AdvertiseAddress, "advertise-addr", "",
		"Address advertised to other brokers and clients",
	)
	flag.Parse()

	explicitly := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { explicitly[f.Name] = true })

	applyEnv := func(flagName, envVar string, target *string) {
		if !explicitly[flagName] {
			if v := os.Getenv(envVar); v != "" {
				*target = v
			}
		}
	}

	applyEnv("data-dir", "COMET_DATA_DIR", &cfg.DataDir)
	applyEnv("addr", "COMET_ADDR", &cfg.ServerAddress)
	applyEnv("etcd", "COMET_ETCD", &etcdEndpoints)
	applyEnv("log-level", "COMET_LOG_LEVEL", &logLevel)
	applyEnv("broker-id", "COMET_BROKER_ID", &cfg.BrokerID)
	applyEnv("advertise-addr", "COMET_ADVERTISE_ADDR", &cfg.AdvertiseAddress)

	cfg.EtcdEndpoints = strings.Split(etcdEndpoints, ",")

	if cfg.AdvertiseAddress == "" {
		cfg.AdvertiseAddress = cfg.ServerAddress
	}

	logger, err := logging.New(logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed creating logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info(
		"Starting comet",
		zap.String("addr", cfg.ServerAddress),
		zap.String("advertiseAddr", cfg.AdvertiseAddress),
		zap.String("brokerID", cfg.BrokerID),
		zap.String("logLevel", logLevel),
	)

	srv, err := server.New(cfg, logger)
	if err != nil {
		logger.Fatal("failed to create server", zap.Error(err))
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	select {
	case err := <-errCh:
		logger.Fatal("server error", zap.Error(err))
	case sig := <-sigCh:
		logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
		srv.Stop()
		logger.Info("server stopped")
	}
}
