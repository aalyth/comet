# Comet -- Documentation

## Table of Contents

1. [Functionality](#1-functionality)
2. [Architecture](#2-architecture)
3. [Implementation](#3-implementation)
4. [Data Model](#4-data-model)
5. [Configuration](#5-configuration)
6. [Technologies and Libraries](#6-technologies-and-libraries)
7. [Design Tradeoffs](#7-design-tradeoffs)
8. [Problems and Solutions](#8-problems-and-solutions)
9. [References](#9-references)

---

## 1. Functionality

Comet is a distributed message streaming broker written in Go. It is modeled
after Apache Kafka and provides the following core functionality:

### Topics and Partitions

- Messages are organized into named streams called **topics**.
- Each topic is divided into one or more **partitions**, enabling parallel
  processing and horizontal scaling.
- Messages within a partition are strictly ordered. Each message is assigned a
  unique, increasing **offset**.

### Producing and Consuming

- **Producers** send messages to a topic. The target partition is determined
  automatically by hashing the message key (CRC32), or by a round-robin fashion
  if no key is provided.
- **Consumers** read messages from a topic using a poll-based approach. The
  read position (offset) is persisted across restarts.

### Consumer Groups

- Multiple consumers can form a **consumer group**. The topic's partitions are
  automatically distributed among group members.
- When a member joins or leaves a consumer group, then **rebalancing** occurs
  -- partitions are reassigned using a round-robin algorithm.
- Offsets are batch committed periodically to etcd, allowing recovery after a
  crash, whilst maintaining stable network throughput.

### Replication

- Each topic can be created with a **replication factor**. Data is written by
  the leader and asynchronously replicated to the specified number of follower
  brokers.
- Replication is fault-tolerant -- if a follower is unreachable, the leader
  continues accepting messages.

### Coordination via etcd

- In multi-broker mode, etcd is used for:
  - Broker registration and discovery
  - Storing topic metadata and partition assignments
  - Storing consumer group offsets
  - Watching for changes and triggering automatic reassignment

### Single-Broker Mode

- The system can operate without etcd -- in this mode all coordination is
  performed in memory. Suitable for development and testing.

---

## 2. Architecture

### Overview

The system consists of the following core components:

```
                    +-------------------+
                    |    Client (Go)    |
                    |  Producer/Consumer|
                    +--------+----------+
                             | gRPC
                             v
+-----------------------------------------------------------+
|                      Broker (gRPC Server)                 |
|                                                           |
|  +-------------+  +----------------+  +----------------+  |
|  |   Storage   |  |  Assignment    |  |    Broker      |  |
|  |   (WAL)     |  |  Manager       |  |    Registry    |  |
|  +-------------+  +----------------+  +----------------+  |
|                                                           |
|  +-------------+  +----------------+  +----------------+  |
|  |   Group     |  |    Peer        |  |   Replication  |  |
|  |   Manager   |  |    Manager     |  |   (async)      |  |
|  +-------------+  +----------------+  +----------------+  |
+-----------------------------------------------------------+
                             |
                             v
                    +-------------------+
                    |       etcd        |
                    |  (coordination)   |
                    +-------------------+
```

### Components

#### gRPC Server (`internal/server/`)

Implements the `BrokerService` defined in `proto/comet/v1/broker.proto`.
Handles all client and inter-broker requests:

- Admin RPCs: `CreateTopic`, `DeleteTopic`, `ListTopics`, `GetMetadata`
- Producing: `Produce`
- Consuming: `Subscribe`, `Poll`, `Unsubscribe`
- Inter-broker: `Consume`, `Replicate`

#### Broker (`internal/broker/`)

The central module that coordinates all other components:

- Handles produce requests -- checks leadership, writes locally, replicates
  to followers
- Manages consumer groups via `GroupManager`
- Watches for changes in the broker set and reassigns partitions

#### Storage (`internal/storage/`)

WAL-based data storage:

- **Storage** -- manages topics and partitions
- **Partition** -- maintains a list of segments and manages the next offset
- **Segment** -- a file with sequential records and an in-memory index for
  fast lookups

#### Broker Registry (`internal/broker/registry.go`)

Discovers and tracks available brokers:

- Registers the current broker in etcd with a lease (TTL 10 seconds)
- Maintains a keepalive for automatic removal on crash
- Watches for changes and notifies other components via a channel
  (`WaitChange`)

#### Assignment Manager (`internal/broker/assignment.go`)

Manages the assignment of partitions to brokers:

- Stores topic metadata in etcd (`/comet/meta/`)
- Stores assignments in etcd (`/comet/assignments/`)
- Computes assignments deterministically using round-robin
- Watches for changes and notifies the broker to create local storage

#### Group Manager (`internal/broker/group.go`)

Manages consumer groups:

- Member registration via etcd
- Poll-based liveness detection (default timeout: 10 seconds)
- Round-robin partition assignment
- Periodic offset commits to etcd

#### Peer Manager (`internal/broker/peers.go`)

Manages gRPC connections to other brokers:

- Lazy connection creation on first use
- Connection caching for reuse
- Used for replication and proxying consume requests

### Multi-Broker Mode

In a multi-broker configuration:

1. Each broker registers in etcd with a unique UUID and advertise address
2. When a topic is created, partitions are distributed among available brokers
3. The leader of each partition writes data locally and replicates it
4. Consumers connect to any broker, which proxies requests for data on
   partitions owned by other brokers

---

## 3. Implementation

### gRPC Service -- BrokerService

The service is defined in `proto/comet/v1/broker.proto` and includes the
following RPCs:

| RPC           | Description                                                         |
| ------------- | ------------------------------------------------------------------- |
| `CreateTopic` | Creates a topic with a given partition count and replication factor |
| `DeleteTopic` | Deletes a topic and its associated data                             |
| `ListTopics`  | Returns a list of all topics                                        |
| `GetMetadata` | Returns broker, topic, and partition leader information             |
| `Produce`     | Writes a batch of records to a topic                                |
| `Subscribe`   | Joins a consumer to a group                                         |
| `Poll`        | Fetches messages for a given consumer                               |
| `Unsubscribe` | Removes a consumer from a group                                     |
| `Consume`     | Reads records from a partition (inter-broker)                       |
| `Replicate`   | Replicates records from leader to follower                          |

### Producer (Client Library)

`TopicProducer` (`pkg/client/producer.go`) is a buffered, asynchronous
producer bound to a single topic:

- **Automatic topic creation** -- on initialization, `CreateTopic` is called
  with retry logic and backoff for multi-broker environments where not all
  brokers may have registered yet.
- **Metadata** -- periodically refreshed (default every 30 seconds) via
  `GetMetadata` to discover partition leaders.
- **Buffering and flush** -- messages are buffered and sent in batches
  (default: every 100ms or when 1000 messages are buffered).
- **Routing** -- messages are grouped by leader broker and sent directly to
  the correct broker.
- **Partition selection** -- by key via CRC32 hashing, or round-robin.
- **Bootstrap reconnect** -- on communication failure with the current
  bootstrap broker, the producer switches to the next available address.
- **Stale connection cleanup** -- on produce failure, the stale connection to
  the broker is closed and metadata is refreshed.

### Consumer (Client Library)

`Consumer` (`pkg/client/consumer.go`) is a poll-based consumer with consumer
group support:

- **Poll loop** -- runs in a separate goroutine for each subscribed topic.
- **Joining** -- calls the `Subscribe` RPC to obtain a member ID and assigned
  partitions.
- **Reading** -- periodically calls the `Poll` RPC for new messages.
- **Rebalancing** -- on receiving `rebalance=true` in the response, the
  consumer continues with updated partitions.
- **Exponential backoff** -- on errors, the retry interval increases (from
  50ms up to a maximum of 5 seconds).
- **Reconnect** -- on an unreachable broker, the consumer switches to the
  next available bootstrap address.

### Replication

The replication flow:

1. The leader writes records to local storage.
2. For each follower, a `Replicate` RPC is sent in a separate goroutine.
3. The follower calls `EnsurePartition` to create local storage (if it does
   not exist) and writes the data.
4. Replication failures are logged but do not block the produce operation --
   the leader has already persisted the data.

### Assignment Watcher

When a new topic is created by another broker, the remaining brokers need to
learn about it and create local storage for their partitions:

1. `AssignmentManager` watches etcd for changes under `/comet/assignments/`
   and `/comet/meta/`.
2. On change, a signal is sent via the `WaitChange()` channel.
3. The broker receives the signal in `watchAssignmentChanges()` and calls
   `ensureLocalPartitions()` to create the needed storage.

---

## 4. Data Model

### Hierarchy

```
Topic
  +-- Partition 0
  |     +-- Segment 00000000000000000000.log
  |     +-- Segment 00000000000000001024.log
  |     +-- ...
  +-- Partition 1
  |     +-- Segment 00000000000000000000.log
  +-- ...
```

### Records

Each message contains:

| Field       | Type  | Description                                 |
| ----------- | ----- | ------------------------------------------- |
| `offset`    | int64 | Unique sequential number within a partition |
| `key`       | bytes | Partitioning key (optional)                 |
| `value`     | bytes | Message payload                             |
| `timestamp` | int64 | Timestamp in nanoseconds                    |

### WAL Format (Segments)

Segments are files with sequential records in the following format:

```
+-------------------+------------------------------+
| Length (4 bytes)  | Protobuf WalRecord (N bytes) |
+-------------------+------------------------------+
| Length (4 bytes)  | Protobuf WalRecord (N bytes) |
+-------------------+------------------------------+
| ...                                              |
+--------------------------------------------------+
```

- The length is stored in big-endian format (4 bytes).
- The record is serialized as a Protocol Buffer `WalRecord`.
- On startup, the index `offset -> file position` is rebuilt from the file.

### File Naming

- Directory structure: `{dataDir}/{topicName}/{partitionID}/`
- Segment files: `{baseOffset:020d}.log`
  (e.g. `00000000000000000000.log`)
- Segment rotation: a new segment is created when `SegmentMaxBytes` (default
  1 GB) is reached.

### In-Memory Index

Each segment maintains a map `offset -> file position` in memory. This
provides O(1) access to any record by offset without needing to scan the
file.

### etcd Key Structure

In multi-broker mode, etcd stores the following keys:

| Path                                         | Contents                                  |
| -------------------------------------------- | ----------------------------------------- |
| `/comet/brokers/{brokerID}`                  | Advertise address of the broker           |
| `/comet/meta/{topicName}`                    | JSON: partition count, replication factor |
| `/comet/assignments/{topicName}`             | JSON: leaders and replicas per partition  |
| `/comet/groups/{topic}/{group}/{memberID}`   | Group member registration                 |
| `/comet/offsets/{topic}/{group}/{partition}` | Committed offset (string)                 |

---

## 5. Configuration

### Broker Parameters

#### CLI Flags and Environment Variables

| Flag               | Env Var                | Default          | Description                             |
| ------------------ | ---------------------- | ---------------- | --------------------------------------- |
| `--data-dir`       | `COMET_DATA_DIR`       | `data`           | Data directory for WAL segments         |
| `--addr`           | `COMET_ADDR`           | `:6174`          | gRPC listen address                     |
| `--etcd`           | `COMET_ETCD`           | `localhost:2379` | Comma-separated etcd endpoints          |
| `--log-level`      | `COMET_LOG_LEVEL`      | `info`           | Log level (debug, info, warn, error)    |
| `--broker-id`      | `COMET_BROKER_ID`      | _(auto UUID)_    | Unique broker identifier                |
| `--advertise-addr` | `COMET_ADVERTISE_ADDR` | same as `--addr` | Address advertised to clients and peers |

#### Internal Constants (`internal/config/config.go`)

| Constant                      | Value      | Description                            |
| ----------------------------- | ---------- | -------------------------------------- |
| `DefaultSegmentMaxBytes`      | 1 GB       | Maximum segment size before rotation   |
| `DefaultSegmentMaxAge`        | 24 hours   | Maximum segment age                    |
| `DefaultIndexIntervalBytes`   | 4 KB       | Indexing interval                      |
| `DefaultOffsetCommitCount`    | 100        | Number of records before offset commit |
| `DefaultOffsetCommitInterval` | 500ms      | Interval for offset commits            |
| `DefaultPollTimeout`          | 10 seconds | Consumer poll timeout                  |

### Producer Configuration

| Parameter                 | Default      | Description                      |
| ------------------------- | ------------ | -------------------------------- |
| `BootstrapAddresses`      | _(required)_ | Broker addresses for connecting  |
| `Topic`                   | _(required)_ | Topic name                       |
| `Partitions`              | _(required)_ | Number of partitions on creation |
| `ReplicationFactor`       | 1            | Replication factor               |
| `BufferSize`              | 1000         | Max messages in buffer           |
| `FlushInterval`           | 100ms        | Automatic flush interval         |
| `BatchSize`               | 100          | Max records per Produce RPC      |
| `MetadataRefreshInterval` | 30s          | Metadata refresh interval        |

### Consumer Configuration

| Parameter            | Default      | Description                                      |
| -------------------- | ------------ | ------------------------------------------------ |
| `BootstrapAddresses` | _(required)_ | Broker addresses for connecting                  |
| `Group`              | _(required)_ | Consumer group name                              |
| `PollInterval`       | 500ms        | Interval between polls when no data is available |
| `MaxPollRecords`     | 100          | Max records per Poll request                     |
| `ChannelBuffer`      | 256          | Message channel buffer size                      |
| `BackoffMin`         | 50ms         | Minimum backoff on error                         |
| `BackoffMax`         | 5s           | Maximum backoff on error                         |
| `BackoffMultiplier`  | 2.0          | Exponential backoff multiplier                   |

---

## 6. Technologies and Libraries

### Core Technologies

| Technology           | Version  | Purpose                                                         |
| -------------------- | -------- | --------------------------------------------------------------- |
| **gRPC**             | v1.78.0  | RPC framework for client-broker and broker-broker communication |
| **Protocol Buffers** | v1.36.11 | Message serialization and gRPC service definition               |
| **etcd**             | v3.6.7   | Distributed coordination, service discovery, metadata storage   |
| **Docker**           | --       | Containerization and deployment via Docker Compose              |

### Go Libraries

| Library                               | Purpose                               |
| ------------------------------------- | ------------------------------------- |
| `go.uber.org/zap` v1.27.0             | High-performance structured logging   |
| `github.com/google/uuid` v1.6.0       | UUID generation for broker IDs        |
| `github.com/pkg/errors` v0.9.1        | Error wrapping with context           |
| `github.com/stretchr/testify` v1.11.1 | Assertions and helpers for unit tests |
| `google.golang.org/grpc`              | gRPC client and server                |
| `google.golang.org/protobuf`          | Go runtime for Protocol Buffers       |
| `go.etcd.io/etcd/client/v3`           | Go client for etcd v3 API             |

### Development Tools

- **protoc** + `protoc-gen-go` + `protoc-gen-go-grpc` -- Go code generation
  from .proto files
- **Make** -- build system (`Makefile` with targets for build, test, proto,
  docker)
- **Docker Compose** -- orchestration of a multi-broker environment for
  testing

---

## 7. Design Tradeoffs

This section documents the key design decisions and their tradeoffs.

### Best-Effort Replication

The leader persists records to its local WAL and then asynchronously pushes
them to followers via the `Replicate` RPC. The leader does **not** wait for
follower acknowledgements and does **not** retry on failure. If a follower is
unreachable (e.g. during cluster startup or a network partition), the records
are not delivered to that follower.

**Implication:** Followers may lag behind or permanently miss records. A
follower that was down during a produce burst will not have those records once
it comes back up -- there is no catch-up mechanism.

**Rationale:** This prioritizes write availability and low latency over strict
durability guarantees across replicas. This is useful for high-throughput use
cases such as real-time price feeds, where such tiny data changes are tolerable
compared to the increased speed.

### At-Least-Once Delivery Semantics

etcd watchers are used to detect broker and assignment changes, but there is
an inherent propagation delay between the event occurring in etcd and the
watcher callback firing on each broker. During a consumer group rebalance:

1. Consumer A is polling partition 0.
2. Consumer B joins the group; a rebalance assigns partition 0 to B.
3. Consumer A's in-flight `Poll` may still return messages from partition 0.
4. Consumer B starts polling partition 0 from the last committed offset.
5. Messages delivered to A in step 3 (but not yet committed) are delivered
   again to B.

The system does **not** deduplicate messages. Consumers may see the same
message more than once.

**Rationale:** Exactly-once semantics require transactional offset commits,
idempotent producers, and consumer-side deduplication -- all of which add
significant complexity. At-least-once is the pragmatic default used by most
streaming systems (including Kafka in its default configuration).

### In-Memory Metadata with Eventual Consistency

Partition assignments, topic metadata, and the broker registry are cached in
memory and synced from etcd via watchers. There is a brief window where the
in-memory state could be stale:

- A newly created topic may not be visible on a remote broker for a short
  period until the assignment watcher fires.
- A broker that has crashed may still appear in the registry until its etcd
  lease expires (up to 10 seconds).

**Rationale:** Reading metadata from the in-memory cache on every produce and
consume request is orders of magnitude faster than querying etcd. The staleness
window is typically milliseconds and is acceptable for particular use cases.

### No Consumer Offset Fencing

When a rebalance occurs, the old consumer's in-flight `Poll` may still commit
offsets for partitions it no longer owns. There is no generation-based fencing
to reject stale offset commits. This can cause slight offset regression: the
new owner reads from the last committed offset, which may be behind where
the old owner actually stopped.

**Rationale:** Generation-based fencing requires storing and validating
generation IDs on every offset commit, adding complexity to the critical
path. The impact is limited to a small number of duplicate messages during
rebalancing, which is already covered by the at-least-once semantics.

### Round-Robin Partition Assignment

Both topic-to-broker assignments (which broker leads which partition) and
partition-to-consumer assignments (which consumer reads which partition) use
simple, deterministic round-robin. Broker IDs or member IDs are sorted
alphabetically, and partition `i` is assigned to entry `i % N`.

**Implication:** This does not account for broker load, partition size,
consumer capacity, or data locality. One broker may end up leading partitions
with very different traffic volumes.

**Rationale:** Round-robin is deterministic (all brokers compute the same
result independently), simple to implement, and easy to reason about. More
sophisticated assignment strategies (e.g. rack-aware, load-based) add
complexity that is not warranted for the project's scope.

---

## 9. Inspiration and Design

- **Apache Kafka** -- primary architectural model for Comet
  - [Kafka Documentation](https://kafka.apache.org/documentation/)
  - [Kafka Design](https://kafka.apache.org/documentation/#design)
  - Concepts: topics, partitions, consumer groups, offsets, replication
