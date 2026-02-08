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
	flag.StringVar(&etcdEndpoints, "etcd", strings.Join(cfg.EtcdEndpoints, ","), "Comma-separated etcd endpoints")
	flag.StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	flag.Parse()

	cfg.EtcdEndpoints = strings.Split(etcdEndpoints, ",")

	logger, err := logging.New(logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("starting comet",
		zap.String("addr", cfg.ServerAddress),
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
