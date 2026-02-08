package broker

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	brokerKeyPrefix       = "/comet/brokers/"
	brokerLeaseTtlSeconds = 10
)

// BrokerRegistry manages broker discovery via etcd. Each broker registers
// itself under /comet/brokers/{brokerID} with its advertise address. An etcd
// lease with keepalive ensures that dead brokers are automatically removed.
// A watcher keeps the in-memory cache of live brokers up to date.
type BrokerRegistry struct {
	brokerID string
	address  string
	etcd     *clientv3.Client

	mu sync.RWMutex
	// brokerID -> address
	brokers map[string]string
	notify  chan struct{}

	logger *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewBrokerRegistry(
	etcd *clientv3.Client,
	brokerID, address string,
	logger *zap.Logger,
) *BrokerRegistry {
	ctx, cancel := context.WithCancel(context.Background())
	return &BrokerRegistry{
		brokerID: brokerID,
		address:  address,
		etcd:     etcd,
		brokers:  make(map[string]string),
		notify:   make(chan struct{}),
		logger:   logger.Named("registry"),
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (r *BrokerRegistry) Register() error {
	if r.etcd == nil {
		r.mu.Lock()
		r.brokers[r.brokerID] = r.address
		r.mu.Unlock()
		r.logger.Info("registered broker (in-memory)", zap.String("id", r.brokerID))
		return nil
	}

	lease, err := r.etcd.Grant(r.ctx, brokerLeaseTtlSeconds)
	if err != nil {
		return errors.Wrap(err, "granting etcd lease")
	}

	key := brokerKeyPrefix + r.brokerID
	_, err = r.etcd.Put(r.ctx, key, r.address, clientv3.WithLease(lease.ID))
	if err != nil {
		return errors.Wrap(err, "registering broker in etcd")
	}

	ch, err := r.etcd.KeepAlive(r.ctx, lease.ID)
	if err != nil {
		return errors.Wrap(err, "starting lease keepalive")
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for range ch {
		}
	}()

	r.syncBrokersFromEtcd()

	r.wg.Add(1)
	go r.watchBrokers()

	r.logger.Info(
		"broker registered",
		zap.String("id", r.brokerID),
		zap.String("address", r.address),
	)
	return nil
}

func (r *BrokerRegistry) BrokerID() string {
	return r.brokerID
}

func (r *BrokerRegistry) Address() string {
	return r.address
}

func (r *BrokerRegistry) LiveBrokers() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cp := make(map[string]string, len(r.brokers))
	for id, addr := range r.brokers {
		cp[id] = addr
	}
	return cp
}

func (r *BrokerRegistry) LiveBrokerIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.brokers))
	for id := range r.brokers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (r *BrokerRegistry) BrokerAddress(brokerID string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	addr, ok := r.brokers[brokerID]
	return addr, ok
}

func (r *BrokerRegistry) BrokerCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.brokers)
}

func (r *BrokerRegistry) WaitChange() <-chan struct{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.notify
}

func (r *BrokerRegistry) Close() error {
	if r.etcd != nil {
		key := brokerKeyPrefix + r.brokerID
		if _, err := r.etcd.Delete(context.Background(), key); err != nil {
			r.logger.Error("failed deregistering broker", zap.Error(err))
		}
	}
	r.cancel()
	r.wg.Wait()
	r.logger.Info("broker registry closed")
	return nil
}

func (r *BrokerRegistry) watchBrokers() {
	defer r.wg.Done()

	wch := r.etcd.Watch(r.ctx, brokerKeyPrefix, clientv3.WithPrefix())
	for {
		select {
		case <-r.ctx.Done():
			return
		case resp, ok := <-wch:
			if !ok {
				return
			}

			for _, ev := range resp.Events {
				key := strings.TrimPrefix(string(ev.Kv.Key), brokerKeyPrefix)
				r.logger.Debug(
					"broker registry event",
					zap.String("type", ev.Type.String()),
					zap.String("brokerID", key),
				)
			}

			r.syncBrokersFromEtcd()

			// signal change by closing old channel and creating new one
			r.mu.Lock()
			close(r.notify)
			r.notify = make(chan struct{})
			r.mu.Unlock()
		}
	}
}

func (r *BrokerRegistry) syncBrokersFromEtcd() {
	resp, err := r.etcd.Get(r.ctx, brokerKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		r.logger.Warn("failed syncing brokers from etcd", zap.Error(err))
		return
	}

	etcdBrokers := make(map[string]string)
	for _, kv := range resp.Kvs {
		id := strings.TrimPrefix(string(kv.Key), brokerKeyPrefix)
		addr := string(kv.Value)
		etcdBrokers[id] = addr
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// remove brokers that are no longer alive
	for id := range r.brokers {
		if _, ok := etcdBrokers[id]; !ok {
			delete(r.brokers, id)
		}
	}

	// add/update brokers from etcd
	for id, addr := range etcdBrokers {
		r.brokers[id] = addr
	}

	r.logger.Debug("brokers synced", zap.Int("count", len(r.brokers)))
}
