package broker

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	memberKeyPrefix = "/comet/groups/"
	offsetKeyPrefix = "/comet/offsets/"
	leaseTtlSeconds = 10
)

type groupKey struct {
	topic string
	group string
}

func (gk *groupKey) memberEtcdKey(groupMemberId string) string {
	return fmt.Sprintf("%s%s/%s/%s", memberKeyPrefix, gk.topic, gk.group, groupMemberId)
}

func (gk *groupKey) etcdKey() string {
	return fmt.Sprintf("%s%s/%s/", memberKeyPrefix, gk.topic, gk.group)
}

type offsetKey struct {
	topic     string
	group     string
	partition int32
}

func (ok *offsetKey) etcdKey() string {
	return fmt.Sprintf("%s%s/%s/%d", offsetKeyPrefix, ok.topic, ok.group, ok.partition)
}

// A concrete consumer group member and it's assigned partitions.
type GroupMember struct {
	ID         string
	partitions []int32
	mu         sync.RWMutex
}

func (m *GroupMember) Partitions() []int32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp := make([]int32, len(m.partitions))
	copy(cp, m.partitions)
	return cp
}

func (m *GroupMember) setPartitions(partitions []int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.partitions = partitions
}

// Metadata about the whole consumer group for a given topic.
type consumerGroup struct {
	mu         sync.Mutex
	members    map[string]*GroupMember
	numParts   int32
	notify     chan struct{}
	leaseID    clientv3.LeaseID
	cancelKeep context.CancelFunc
}

func newConsumerGroup(numPartitions int32) *consumerGroup {
	return &consumerGroup{
		members:  make(map[string]*GroupMember),
		numParts: numPartitions,
		notify:   make(chan struct{}),
	}
}

type pendingOffset struct {
	partition int32
	offset    int64
}

// Manages all consumer groups over the different topics. Each client's offset
// information is stored within etcd.
type GroupManager struct {
	// In-memory cache of the information for each topic's consumer groups.
	// Data is automatically updated via etcd watchers, i.e. when another
	// instance updates the data within etcd, we get notified to update our
	// in-memory cache.
	mu     sync.Mutex
	groups map[groupKey]*consumerGroup

	// For more optimal network usage, offsets are flushed periodically to
	// etcd (not on every single update).
	etcd                 *clientv3.Client
	offsetCommitCount    int
	offsetCommitInterval time.Duration

	pendingMu      sync.Mutex
	pendingOffsets map[groupKey][]pendingOffset

	logger *zap.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewGroupManager(
	etcd *clientv3.Client,
	offsetCommitCount int,
	offsetCommitInterval time.Duration,
	logger *zap.Logger,
) *GroupManager {
	ctx, cancel := context.WithCancel(context.Background())
	l := logger.Named("groups")
	gm := &GroupManager{
		groups:               make(map[groupKey]*consumerGroup),
		etcd:                 etcd,
		offsetCommitCount:    offsetCommitCount,
		offsetCommitInterval: offsetCommitInterval,
		pendingOffsets:       make(map[groupKey][]pendingOffset),
		logger:               l,
		ctx:                  ctx,
		cancel:               cancel,
	}

	gm.wg.Add(1)
	go gm.offsetFlusher()

	if etcd != nil {
		gm.wg.Add(1)
		go gm.watchMembers()
		l.Info("etcd watcher started")
	}

	l.Info("Group manager initialized", zap.Bool("etcd", etcd != nil))
	return gm
}

func (gm *GroupManager) Join(
	topic, group string,
	numPartitions int32,
) (*GroupMember, func(), error) {
	gk := groupKey{topic: topic, group: group}
	memberId := uuid.New().String()

	gm.mu.Lock()
	cg, ok := gm.groups[gk]
	if !ok {
		cg = newConsumerGroup(numPartitions)
		gm.groups[gk] = cg
		gm.logger.Info(
			"Consumer group created",
			zap.String("topic", topic),
			zap.String("group", group),
			zap.Int32("partitions", numPartitions),
		)
	}
	gm.mu.Unlock()

	member := &GroupMember{ID: memberId}

	cg.mu.Lock()
	cg.members[memberId] = member
	cg.mu.Unlock()

	if gm.etcd != nil {
		if err := gm.registerMember(gk, memberId); err != nil {
			cg.mu.Lock()
			delete(cg.members, memberId)
			cg.mu.Unlock()
			return nil, nil, errors.Wrap(err, "registering member in etcd")
		}
	}

	gm.logger.Info(
		"Member joined",
		zap.String("topic", topic),
		zap.String("group", group),
		zap.String("memberId", memberId),
	)

	gm.rebalance(gk, cg)
	cleanup := func() {
		cg.mu.Lock()
		delete(cg.members, memberId)
		cg.mu.Unlock()

		if gm.etcd != nil {
			etcdKey := gk.memberEtcdKey(memberId)
			if _, err := gm.etcd.Delete(context.Background(), etcdKey); err != nil {
				gm.logger.Error(
					"Failed deleting etcd gk",
					zap.String("gk", etcdKey),
					zap.Error(err),
				)
			}
		}

		gm.logger.Info(
			"member left",
			zap.String("topic", topic),
			zap.String("group", group),
			zap.String("memberId", memberId),
		)

		gm.rebalance(gk, cg)
	}

	return member, cleanup, nil
}

// GetOffset returns the committed offset for a partition in a consumer group.
func (gm *GroupManager) GetOffset(topic, group string, partition int32) (int64, error) {
	if gm.etcd != nil {
		etcdKey := fmt.Sprintf(
			"%s%s/%s/%d", offsetKeyPrefix, topic, group, partition,
		)

		resp, err := gm.etcd.Get(gm.ctx, etcdKey)
		if err != nil {
			return 0, fmt.Errorf("get offset from etcd: %w", err)
		}

		if len(resp.Kvs) == 0 {
			return 0, nil
		}

		var offset int64
		fmt.Sscanf(string(resp.Kvs[0].Value), "%d", &offset)
		return offset, nil
	}

	// In-memory mode: offsets start at 0.
	return 0, nil
}

// SetOffset buffers an offset commit. It will be flushed to etcd either when
// the batch reaches offsetCommitCount or after offsetCommitInterval.
func (gm *GroupManager) SetOffset(topic, group string, partition int32, offset int64) {
	key := groupKey{topic: topic, group: group}

	gm.pendingMu.Lock()
	gm.pendingOffsets[key] = append(
		gm.pendingOffsets[key], pendingOffset{
			partition: partition,
			offset:    offset,
		},
	)
	shouldFlush := len(gm.pendingOffsets[key]) >= gm.offsetCommitCount
	gm.pendingMu.Unlock()

	if shouldFlush {
		gm.flushOffsets()
	}
}

// WaitRebalance returns a channel that is closed when a rebalance occurs.
// Callers should re-read their partition assignments after the channel fires.
func (gm *GroupManager) WaitRebalance(topic, group string) <-chan struct{} {
	key := groupKey{topic: topic, group: group}

	gm.mu.Lock()
	cg, ok := gm.groups[key]
	gm.mu.Unlock()
	if !ok {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	cg.mu.Lock()
	defer cg.mu.Unlock()
	return cg.notify
}

// Close flushes pending offsets and stops background goroutines.
func (gm *GroupManager) Close() error {
	gm.flushOffsets()
	gm.cancel()
	gm.wg.Wait()
	gm.logger.Info("group manager closed")
	return nil
}

// Round-robin assignment of partitions among the group members (partition `i`
// goes to consumer `i % len(groupMembers)`). The updates signal all `etcd`
// waiters to update their in-memory caches.
func (gm *GroupManager) rebalance(key groupKey, cg *consumerGroup) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	memberIDs := make([]string, 0, len(cg.members))
	for id := range cg.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	// clear all assignments
	for _, m := range cg.members {
		m.setPartitions(nil)
	}

	if len(memberIDs) > 0 {
		assignments := make(map[string][]int32)
		for p := int32(0); p < cg.numParts; p++ {
			owner := memberIDs[int(p)%len(memberIDs)]
			assignments[owner] = append(assignments[owner], p)
		}
		for id, parts := range assignments {
			cg.members[id].setPartitions(parts)
		}

		gm.logger.Info(
			"rebalance complete",
			zap.String("topic", key.topic),
			zap.String("group", key.group),
			zap.Int("members", len(memberIDs)),
			zap.Int32("partitions", cg.numParts),
		)
	}

	// signal rebalance by closing the old channel and creating a new one
	close(cg.notify)
	cg.notify = make(chan struct{})
}

// registerMember creates an etcd lease and puts the member key.
func (gm *GroupManager) registerMember(gk groupKey, memberId string) error {
	lease, err := gm.etcd.Grant(gm.ctx, leaseTtlSeconds)
	if err != nil {
		return errors.Wrap(err, "granting etcd lease")
	}

	etcdKey := gk.memberEtcdKey(memberId)
	_, err = gm.etcd.Put(gm.ctx, etcdKey, memberId, clientv3.WithLease(lease.ID))
	if err != nil {
		return errors.Wrap(err, "putting group member etcd gk")
	}

	keepCtx, keepCancel := context.WithCancel(gm.ctx)
	ch, err := gm.etcd.KeepAlive(keepCtx, lease.ID)
	if err != nil {
		keepCancel()
		return fmt.Errorf("keep alive: %w", err)
	}

	// handle keepalive responses in the background.
	gm.wg.Add(1)
	go func() {
		defer gm.wg.Done()
		for range ch {
		}
	}()

	// Store cancel so cleanup can stop the keepalive.
	gm.mu.Lock()
	cg := gm.groups[gk]
	gm.mu.Unlock()

	cg.mu.Lock()
	cg.leaseID = lease.ID
	cg.cancelKeep = keepCancel
	cg.mu.Unlock()

	gm.logger.Debug(
		"etcd member registered",
		zap.String("gk", etcdKey),
		zap.String("memberId", memberId),
	)

	return nil
}

// watchMembers watches the member key prefix in etcd and triggers rebalances
// when members join or leave.
func (gm *GroupManager) watchMembers() {
	defer gm.wg.Done()

	wch := gm.etcd.Watch(gm.ctx, memberKeyPrefix, clientv3.WithPrefix())
	for {
		select {
		case <-gm.ctx.Done():
			return
		case resp, ok := <-wch:
			if !ok {
				return
			}

			for _, ev := range resp.Events {
				// Extract topic and group from the key:
				// /comet/groups/<topic>/<group>/<memberID>
				parts := strings.TrimPrefix(string(ev.Kv.Key), memberKeyPrefix)
				segments := strings.SplitN(parts, "/", 3)
				if len(segments) < 2 {
					continue
				}
				key := groupKey{topic: segments[0], group: segments[1]}

				gm.logger.Debug(
					"etcd watch event",
					zap.String("type", ev.Type.String()),
					zap.String("topic", key.topic),
					zap.String("group", key.group),
				)

				gm.mu.Lock()
				cg, ok := gm.groups[key]
				gm.mu.Unlock()
				if !ok {
					continue
				}

				gm.syncMembersFromEtcd(key, cg)
				gm.rebalance(key, cg)
			}
		}
	}
}

func (gm *GroupManager) syncMembersFromEtcd(gk groupKey, cg *consumerGroup) {
	prefix := gk.etcdKey()
	resp, err := gm.etcd.Get(gm.ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		gm.logger.Warn("Failed syncing members from etcd", zap.Error(err))
		return
	}

	etcdMembers := make(map[string]bool)
	for _, kv := range resp.Kvs {
		memberID := string(kv.Value)
		etcdMembers[memberID] = true
	}

	cg.mu.Lock()
	defer cg.mu.Unlock()

	for id := range cg.members {
		if !etcdMembers[id] {
			delete(cg.members, id)
		}
	}

	for id := range etcdMembers {
		if _, ok := cg.members[id]; !ok {
			cg.members[id] = &GroupMember{ID: id}
		}
	}
}

func (gm *GroupManager) offsetFlusher() {
	defer gm.wg.Done()

	ticker := time.NewTicker(gm.offsetCommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			gm.flushOffsets()
		case <-gm.ctx.Done():
			return
		}
	}
}

// Flush all pending offsets to `etcd` in a single transaction.
func (gm *GroupManager) flushOffsets() {
	gm.pendingMu.Lock()
	pending := gm.pendingOffsets
	gm.pendingOffsets = make(map[groupKey][]pendingOffset)
	gm.pendingMu.Unlock()

	if gm.etcd == nil || len(pending) == 0 {
		return
	}

	// Collect the latest offset per (group, topic, partition).
	latest := make(map[offsetKey]int64)
	for key, offsets := range pending {
		for _, po := range offsets {
			ok := offsetKey{topic: key.topic, group: key.group, partition: po.partition}
			if existing, found := latest[ok]; !found || po.offset > existing {
				latest[ok] = po.offset
			}
		}
	}

	// Write to etcd in a single batch.
	ops := make([]clientv3.Op, 0, len(latest))
	for ok, offset := range latest {
		etcdKey := fmt.Sprintf(
			"%s%s/%s/%d", offsetKeyPrefix, ok.topic, ok.group, ok.partition,
		)
		ops = append(ops, clientv3.OpPut(etcdKey, fmt.Sprintf("%d", offset)))
	}

	if len(ops) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := gm.etcd.Txn(ctx).Then(ops...).Commit()
		if err != nil {
			gm.logger.Warn(
				"failed to flush offsets to etcd",
				zap.Error(err),
				zap.Int("ops", len(ops)),
			)
		} else {
			gm.logger.Debug("offsets flushed to etcd", zap.Int("ops", len(ops)))
		}
	}
}
