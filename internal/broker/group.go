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

type GroupMember struct {
	ID         string
	partitions []int32
	generation int64
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

func (m *GroupMember) Generation() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.generation
}

func (m *GroupMember) setGeneration(gen int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.generation = gen
}

type consumerGroup struct {
	mu         sync.Mutex
	members    map[string]*GroupMember
	numParts   int32
	generation int64
	lastPoll   map[string]time.Time
}

func newConsumerGroup(numPartitions int32) *consumerGroup {
	return &consumerGroup{
		members:  make(map[string]*GroupMember),
		numParts: numPartitions,
		lastPoll: make(map[string]time.Time),
	}
}

type pendingOffset struct {
	partition int32
	offset    int64
}

// GroupManager manages all consumer groups. In multi-broker mode, membership
// is coordinated via etcd. Poll-based liveness: if a member does not poll
// within pollTimeout, it is evicted and a rebalance is triggered.
type GroupManager struct {
	mu     sync.Mutex
	groups map[groupKey]*consumerGroup

	etcd                 *clientv3.Client
	offsetCommitCount    int
	offsetCommitInterval time.Duration
	pollTimeout          time.Duration

	pendingMu      sync.Mutex
	pendingOffsets map[groupKey][]pendingOffset

	offsetsMu sync.RWMutex
	// memberID -> partition -> offset
	offsets map[string]map[int32]int64

	logger *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewGroupManager(
	etcd *clientv3.Client,
	offsetCommitCount int,
	offsetCommitInterval time.Duration,
	pollTimeout time.Duration,
	logger *zap.Logger,
) *GroupManager {
	ctx, cancel := context.WithCancel(context.Background())
	l := logger.Named("groups")
	gm := &GroupManager{
		groups:               make(map[groupKey]*consumerGroup),
		etcd:                 etcd,
		offsetCommitCount:    offsetCommitCount,
		offsetCommitInterval: offsetCommitInterval,
		pollTimeout:          pollTimeout,
		pendingOffsets:       make(map[groupKey][]pendingOffset),
		offsets:              make(map[string]map[int32]int64),
		logger:               l,
		ctx:                  ctx,
		cancel:               cancel,
	}

	gm.wg.Add(1)
	go gm.offsetFlusher()

	gm.wg.Add(1)
	go gm.pollTimeoutChecker()

	if etcd != nil {
		gm.wg.Add(1)
		go gm.watchMembers()
		l.Info("etcd watcher started")
	}

	l.Info(
		"Group manager initialized",
		zap.Bool("etcd", etcd != nil),
		zap.Duration("pollTimeout", pollTimeout),
	)
	return gm
}

// Join adds a member to a consumer group and triggers a rebalance. Returns the
// member, its assigned partitions, the current generation, and a cleanup func.
func (gm *GroupManager) Join(
	topic, group string,
	numPartitions int32,
) (*GroupMember, int64, error) {
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

	cg.mu.Lock()
	member := &GroupMember{ID: memberId}
	cg.members[memberId] = member
	cg.lastPoll[memberId] = time.Now()
	cg.mu.Unlock()

	if gm.etcd != nil {
		if err := gm.registerMember(gk, memberId); err != nil {
			cg.mu.Lock()
			delete(cg.members, memberId)
			delete(cg.lastPoll, memberId)
			cg.mu.Unlock()
			return nil, 0, errors.Wrap(err, "registering member in etcd")
		}
	}

	gm.logger.Info(
		"Member joined",
		zap.String("topic", topic),
		zap.String("group", group),
		zap.String("memberId", memberId),
	)

	gen := gm.rebalance(gk, cg)
	member.setGeneration(gen)

	// Initialize offsets for this member.
	gm.initMemberOffsets(memberId, topic, group, member.Partitions())

	return member, gen, nil
}

// Poll validates a member is alive and resets its timeout timer. Returns
// whether a rebalance has occurred and the current generation.
func (gm *GroupManager) Poll(
	topic, group, memberID string,
) (partitions []int32, rebalance bool, generation int64, err error) {
	gk := groupKey{topic: topic, group: group}

	gm.mu.Lock()
	cg, ok := gm.groups[gk]
	gm.mu.Unlock()
	if !ok {
		return nil, false, 0, fmt.Errorf("consumer group %s/%s not found", topic, group)
	}

	cg.mu.Lock()
	member, ok := cg.members[memberID]
	if !ok {
		cg.mu.Unlock()
		return nil, false, 0, fmt.Errorf(
			"member %s not found in group %s/%s", memberID, topic, group,
		)
	}
	cg.lastPoll[memberID] = time.Now()
	gen := cg.generation
	cg.mu.Unlock()

	memberGen := member.Generation()
	needsRebalance := memberGen != gen

	if needsRebalance {
		member.setGeneration(gen)
		// Re-initialize offsets for new partition assignment.
		gm.initMemberOffsets(memberID, topic, group, member.Partitions())
	}

	return member.Partitions(), needsRebalance, gen, nil
}

// Leave removes a member from its consumer group and triggers a rebalance.
func (gm *GroupManager) Leave(topic, group, memberID string) error {
	gk := groupKey{topic: topic, group: group}

	gm.mu.Lock()
	cg, ok := gm.groups[gk]
	gm.mu.Unlock()
	if !ok {
		return fmt.Errorf("consumer group %s/%s not found", topic, group)
	}

	cg.mu.Lock()
	_, ok = cg.members[memberID]
	if !ok {
		cg.mu.Unlock()
		return fmt.Errorf("member %s not found", memberID)
	}
	delete(cg.members, memberID)
	delete(cg.lastPoll, memberID)
	cg.mu.Unlock()

	// Clean up offsets.
	gm.offsetsMu.Lock()
	delete(gm.offsets, memberID)
	gm.offsetsMu.Unlock()

	if gm.etcd != nil {
		etcdKey := gk.memberEtcdKey(memberID)
		if _, err := gm.etcd.Delete(context.Background(), etcdKey); err != nil {
			gm.logger.Error("Failed deleting member from etcd", zap.Error(err))
		}
	}

	gm.logger.Info(
		"member left",
		zap.String("topic", topic),
		zap.String("group", group),
		zap.String("memberId", memberID),
	)

	gm.rebalance(gk, cg)
	return nil
}

// GetMemberOffset returns the current offset for a member's partition.
func (gm *GroupManager) GetMemberOffset(memberID string, partition int32) int64 {
	gm.offsetsMu.RLock()
	defer gm.offsetsMu.RUnlock()
	if parts, ok := gm.offsets[memberID]; ok {
		return parts[partition]
	}
	return 0
}

// SetMemberOffset updates the offset for a member's partition.
func (gm *GroupManager) SetMemberOffset(memberID string, partition int32, offset int64) {
	gm.offsetsMu.Lock()
	if gm.offsets[memberID] == nil {
		gm.offsets[memberID] = make(map[int32]int64)
	}
	gm.offsets[memberID][partition] = offset
	gm.offsetsMu.Unlock()
}

// SetOffset buffers an offset commit for periodic flushing to etcd.
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

// GetOffset reads the committed offset for a partition from etcd, or 0.
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

	return 0, nil
}

func (gm *GroupManager) Close() error {
	gm.flushOffsets()
	gm.cancel()
	gm.wg.Wait()
	gm.logger.Info("group manager closed")
	return nil
}

func (gm *GroupManager) initMemberOffsets(memberID, topic, group string, partitions []int32) {
	gm.offsetsMu.Lock()
	offsets := make(map[int32]int64, len(partitions))
	for _, p := range partitions {
		offset, err := gm.GetOffset(topic, group, p)
		if err != nil {
			// default reading from the earliest offset
			offset = 0
		}
		offsets[p] = offset
	}
	gm.offsets[memberID] = offsets
	gm.offsetsMu.Unlock()
}

// rebalance uses round-robbin to assign partitions among group members.
// Returns the new generation number.
func (gm *GroupManager) rebalance(key groupKey, cg *consumerGroup) int64 {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	cg.generation++

	memberIDs := make([]string, 0, len(cg.members))
	for id := range cg.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

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
			"Rebalance complete",
			zap.String("topic", key.topic),
			zap.String("group", key.group),
			zap.Int("members", len(memberIDs)),
			zap.Int32("partitions", cg.numParts),
			zap.Int64("generation", cg.generation),
		)
	}

	return cg.generation
}

func (gm *GroupManager) registerMember(gk groupKey, memberId string) error {
	lease, err := gm.etcd.Grant(gm.ctx, leaseTtlSeconds)
	if err != nil {
		return errors.Wrap(err, "granting etcd lease")
	}

	etcdKey := gk.memberEtcdKey(memberId)
	_, err = gm.etcd.Put(gm.ctx, etcdKey, memberId, clientv3.WithLease(lease.ID))
	if err != nil {
		return errors.Wrap(err, "putting group member key")
	}

	keepCtx, keepCancel := context.WithCancel(gm.ctx)
	ch, err := gm.etcd.KeepAlive(keepCtx, lease.ID)
	if err != nil {
		keepCancel()
		return errors.Wrap(err, "keep alive")
	}

	gm.wg.Add(1)
	go func() {
		defer gm.wg.Done()
		defer keepCancel()
		for range ch {
		}
	}()

	gm.logger.Debug(
		"etcd member registered",
		zap.String("key", etcdKey),
		zap.String("memberId", memberId),
	)

	return nil
}

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
			delete(cg.lastPoll, id)
		}
	}

	for id := range etcdMembers {
		if _, ok := cg.members[id]; !ok {
			cg.members[id] = &GroupMember{ID: id}
			cg.lastPoll[id] = time.Now()
		}
	}
}

func (gm *GroupManager) pollTimeoutChecker() {
	defer gm.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			gm.checkPollTimeouts()
		case <-gm.ctx.Done():
			return
		}
	}
}

func (gm *GroupManager) checkPollTimeouts() {
	gm.mu.Lock()
	groups := make(map[groupKey]*consumerGroup, len(gm.groups))
	for k, v := range gm.groups {
		groups[k] = v
	}
	gm.mu.Unlock()

	now := time.Now()
	for gk, cg := range groups {
		var expired []string

		cg.mu.Lock()
		for memberID, lastPoll := range cg.lastPoll {
			if now.Sub(lastPoll) > gm.pollTimeout {
				expired = append(expired, memberID)
			}
		}

		for _, memberID := range expired {
			delete(cg.members, memberID)
			delete(cg.lastPoll, memberID)
			gm.logger.Info(
				"member evicted (poll timeout)",
				zap.String("topic", gk.topic),
				zap.String("group", gk.group),
				zap.String("memberId", memberID),
			)

			gm.offsetsMu.Lock()
			delete(gm.offsets, memberID)
			gm.offsetsMu.Unlock()
		}
		cg.mu.Unlock()

		if len(expired) > 0 {
			// remove from etcd
			if gm.etcd != nil {
				for _, memberID := range expired {
					etcdKey := gk.memberEtcdKey(memberID)
					if _, err := gm.etcd.Delete(
						context.Background(), etcdKey,
					); err != nil {
						gm.logger.Error(
							"failed deleting expired member from etcd",
							zap.Error(err),
						)
					}
				}
			}
			gm.rebalance(gk, cg)
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

func (gm *GroupManager) flushOffsets() {
	gm.pendingMu.Lock()
	pending := gm.pendingOffsets
	gm.pendingOffsets = make(map[groupKey][]pendingOffset)
	gm.pendingMu.Unlock()

	if gm.etcd == nil || len(pending) == 0 {
		return
	}

	latest := make(map[offsetKey]int64)
	for key, offsets := range pending {
		for _, po := range offsets {
			ok := offsetKey{topic: key.topic, group: key.group, partition: po.partition}
			if existing, found := latest[ok]; !found || po.offset > existing {
				latest[ok] = po.offset
			}
		}
	}

	ops := make([]clientv3.Op, 0, len(latest))
	for ok, offset := range latest {
		ops = append(ops, clientv3.OpPut(ok.etcdKey(), fmt.Sprintf("%d", offset)))
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
