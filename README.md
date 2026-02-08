# Comet

Comet is a distributed message streaming broker written in Go. It provides
topic-based publish/subscribe messaging with partitioned topics, leader-follower
replication, consumer groups, and etcd-based cluster coordination.

## Features

- **Partitioned topics** -- messages are distributed across partitions for parallel processing
- **Leader-follower replication** -- configurable replication factor per topic
- **Consumer groups** -- automatic partition assignment and rebalancing across group members
- **etcd coordination** -- broker discovery, partition assignments, and offset storage
- **gRPC API** -- all client-broker and broker-broker communication uses gRPC
- **Client library** -- Go producer and consumer with automatic metadata refresh, batching, and failover
- **Single-broker mode** -- works without etcd using in-memory coordination

## Project Structure

```
comet/
  cmd/comet/             Entry point for the broker binary
  internal/
    broker/              Core broker logic, replication, consumer groups, assignments
    config/              Configuration defaults and types
    server/              gRPC server implementing the BrokerService
    storage/             WAL-based storage: topics, partitions, segments
    logging/             Zap logger setup
  pkg/client/            Client library (producer, consumer, admin)
  proto/comet/v1/        Protocol Buffer definitions and generated code
  examples/
    price-producer/      Example: stock price update producer
    price-alert/         Example: price alert consumer
```

## Quick Start

### Prerequisites

- Go 1.25+
- etcd (for multi-broker mode)
- Docker and Docker Compose (for containerized deployment)

### Run locally (single broker)

```bash
make build
./bin/comet --addr :6174 --etcd ""
```

### Run with Docker Compose (3-broker cluster)

```bash
make docker-up
```

This starts etcd, three broker instances, a price producer, and two price alert
consumers. To stop:

```bash
make docker-down
```

### Run the examples locally

```bash
make example
```

This starts the price producer and price alert consumer against a local broker.

## Configuration

Comet can be configured via CLI flags or environment variables. Environment
variables take effect when the corresponding flag is not explicitly set.

| Flag               | Env Var                | Default          | Description                             |
| ------------------ | ---------------------- | ---------------- | --------------------------------------- |
| `--data-dir`       | `COMET_DATA_DIR`       | `data`           | Data directory for WAL segments         |
| `--addr`           | `COMET_ADDR`           | `:6174`          | gRPC listen address                     |
| `--etcd`           | `COMET_ETCD`           | `localhost:2379` | Comma-separated etcd endpoints          |
| `--log-level`      | `COMET_LOG_LEVEL`      | `info`           | Log level (debug, info, warn, error)    |
| `--broker-id`      | `COMET_BROKER_ID`      | _(auto UUID)_    | Unique broker identifier                |
| `--advertise-addr` | `COMET_ADVERTISE_ADDR` | same as `--addr` | Address advertised to clients and peers |

## Examples

### Producer

```go
cfg := client.DefaultProducerConfig(
    client.ParseAddresses("broker1:6174,broker2:6174"),
    "my-topic",
    3, // partitions
)
cfg.ReplicationFactor = 2

producer, err := client.NewTopicProducer(cfg)
// handle err

err = producer.Send([]byte("key"), []byte("value"))
// handle err

err = producer.Close()
```

The producer auto-creates the topic on first use, discovers partition leaders
via metadata, and batches messages for efficiency.

### Consumer

```go
cfg := client.DefaultConsumerConfig(
    client.ParseAddresses("broker1:6174,broker2:6174"),
    "my-group",
)

consumer, err := client.NewConsumer(cfg)
// handle err

err = consumer.Subscribe("my-topic", func(msg *client.Message) {
    fmt.Printf("partition=%d offset=%d value=%s\n",
        msg.Partition, msg.Offset, msg.Value)
})
// handle err

// consumer.Close() to shut down
```

Multiple consumers in the same group automatically share partitions. When a
member joins or leaves, partitions are rebalanced.

### Admin

```go
// Create a topic
client.CreateTopic("localhost:6174", "my-topic", 3, 2)

// List topics
topics, _ := client.ListTopics("localhost:6174")

// Delete a topic
client.DeleteTopic("localhost:6174", "my-topic")
```
