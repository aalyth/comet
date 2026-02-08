package client

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProduceConsume(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestTopicProducer(t, addr, "test", 1)
	require.NoError(t, producer.Send([]byte("k1"), []byte("v1")))
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr, "test-group")
	ch, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 1, 2*time.Second)
	require.NoError(t, err)

	assertMessage(t, messages[0], Expect("k1", "v1", 0, "test"))
}

func TestMultipleMessagesInOrder(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestTopicProducer(t, addr, "ordered", 1)
	var expected []MessageExpectation
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		value := fmt.Sprintf("value-%d", i)
		require.NoError(t, producer.Send([]byte(key), []byte(value)))
		expected = append(expected, Expect(key, value, int64(i), "ordered"))
	}
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr, "ordered-group")
	ch, err := consumer.SubscribeChan("ordered")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 10, 2*time.Second)
	require.NoError(t, err)

	assertMessages(t, messages, expected)
}

func TestMultiplePartitions(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestTopicProducer(t, addr, "spread", 4)
	for i := 0; i < 20; i++ {
		value := []byte(fmt.Sprintf("msg-%d", i))
		require.NoError(t, producer.Send(nil, value))
	}
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr, "spread-group")
	ch, err := consumer.SubscribeChan("spread")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 20, 2*time.Second)
	require.NoError(t, err)

	partitionCounts := make(map[int32]int)
	for _, msg := range messages {
		partitionCounts[msg.Partition]++
	}

	assert.Greater(
		t, len(partitionCounts), 1, "Messages should be distributed across partitions",
	)
}

func TestKeyedMessagesToSamePartition(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestTopicProducer(t, addr, "keyed", 4)
	for i := 0; i < 10; i++ {
		require.NoError(
			t,
			producer.Send(
				[]byte("same-key"), []byte(fmt.Sprintf("msg-%d", i)),
			),
		)
	}
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr, "keyed-group")
	ch, err := consumer.SubscribeChan("keyed")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 10, 2*time.Second)
	require.NoError(t, err)

	assertAllMessagesFromSamePartition(t, messages)
}

func TestSubscribeWithHandler(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestTopicProducer(t, addr, "test", 1)
	var expected []MessageExpectation
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("v%d", i)
		require.NoError(t, producer.Send([]byte(key), []byte(value)))
		expected = append(expected, Expect(key, value, int64(i), "test"))
	}
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr, "handler-group")

	var mu sync.Mutex
	var messages []*Message
	handler := func(msg *Message) {
		mu.Lock()
		messages = append(messages, msg)
		mu.Unlock()
	}

	require.NoError(t, consumer.Subscribe("test", handler))

	waitFor(
		t, 2*time.Second, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(messages) >= 3
		},
	)

	mu.Lock()
	defer mu.Unlock()

	assertMessages(t, messages, expected)
}

func TestUnsubscribeAndResubscribe(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestTopicProducer(t, addr, "test", 1)

	// produce first batch
	for i := 0; i < 2; i++ {
		require.NoError(t, producer.Send(nil, []byte(fmt.Sprintf("batch1-%d", i))))
	}
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr, "resub-group")
	ch, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 2, 2*time.Second)
	require.NoError(t, err)
	assert.Len(t, messages, 2)

	require.NoError(t, consumer.Unsubscribe("test"))

	// produce second batch
	for i := 0; i < 2; i++ {
		require.NoError(t, producer.Send(nil, []byte(fmt.Sprintf("batch2-%d", i))))
	}
	require.NoError(t, producer.Flush())

	// resubscribe — in in-memory mode offsets reset, so we get all 4 messages
	ch2, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	allMessages, err := collectMessages(ch2, 4, 2*time.Second)
	require.NoError(t, err)
	assert.Len(t, allMessages, 4)
}

func TestProducerCloseFlushesBuffered(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestTopicProducer(
		t, addr, "test", 1, func(cfg *ProducerConfig) {
			cfg.FlushInterval = 10 * time.Second
		},
	)

	for i := 0; i < 5; i++ {
		require.NoError(t, producer.Send(nil, []byte(fmt.Sprintf("msg-%d", i))))
	}

	require.NoError(t, producer.Close())

	consumer := newTestConsumer(t, addr, "flush-group")
	ch, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 5, 2*time.Second)
	require.NoError(t, err)
	assert.Len(t, messages, 5)
}

func TestTopicProducerCreatesTopicOnStartup(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestTopicProducer(t, addr, "auto-created", 3)

	// verify the topic exists via admin function
	topics, err := ListTopics(addr)
	require.NoError(t, err)

	found := false
	for _, topic := range topics {
		if topic.Name == "auto-created" && topic.Partitions == 3 {
			found = true
			break
		}
	}
	assert.True(t, found, "topic 'auto-created' should exist with 3 partitions")

	// produce and consume to verify it works
	require.NoError(t, producer.Send([]byte("k"), []byte("v")))
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr, "auto-group")
	ch, err := consumer.SubscribeChan("auto-created")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 1, 2*time.Second)
	require.NoError(t, err)
	assert.Len(t, messages, 1)
}

func TestSubscribeWaitsForTopicCreation(t *testing.T) {
	addr := startIntegrationServer(t)

	consumer := newTestConsumer(t, addr, "wait-group")
	ch, err := consumer.SubscribeChan("delayed-topic")
	require.NoError(t, err)

	// wait and then create the topic and produce
	time.Sleep(500 * time.Millisecond)

	producer := newTestTopicProducer(t, addr, "delayed-topic", 1)
	require.NoError(t, producer.Send([]byte("k"), []byte("hello")))
	require.NoError(t, producer.Flush())

	messages, err := collectMessages(ch, 1, 5*time.Second)
	require.NoError(t, err)
	assert.Len(t, messages, 1)
	assert.Equal(t, "hello", string(messages[0].Value))
}

func TestConcurrentProducers(t *testing.T) {
	addr := startIntegrationServer(t)

	createTopic(t, addr, "concurrent", 1)

	var wg sync.WaitGroup
	numProducers := 5
	messagesPerProducer := 20

	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()

			producer := newTestTopicProducer(t, addr, "concurrent", 1)
			for j := 0; j < messagesPerProducer; j++ {
				value := []byte(fmt.Sprintf("producer-%d-msg-%d", producerID, j))
				require.NoError(t, producer.Send(nil, value))
			}
			require.NoError(t, producer.Flush())
		}(i)
	}

	wg.Wait()

	consumer := newTestConsumer(t, addr, "concurrent-group")
	ch, err := consumer.SubscribeChan("concurrent")
	require.NoError(t, err)

	expectedTotal := numProducers * messagesPerProducer
	messages, err := collectMessages(ch, expectedTotal, 5*time.Second)
	require.NoError(t, err)
	assert.Len(t, messages, expectedTotal)
}

func TestProducerBuffering(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestTopicProducer(
		t, addr, "test", 1, func(cfg *ProducerConfig) {
			cfg.FlushInterval = 10 * time.Second
		},
	)

	for i := 0; i < 5; i++ {
		require.NoError(t, producer.Send([]byte("key"), []byte("value")))
	}

	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr, "buffer-group")
	ch, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 5, 2*time.Second)
	require.NoError(t, err)
	assert.Len(t, messages, 5)
}

func TestSendAfterClose(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestTopicProducer(t, addr, "test", 1)
	require.NoError(t, producer.Close())

	err := producer.Send([]byte("key"), []byte("value"))
	assert.Error(t, err)
}

func TestFlushEmptyBuffer(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestTopicProducer(t, addr, "test", 1)
	assert.NoError(t, producer.Flush())
}

func TestAutoFlush(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestTopicProducer(
		t, addr, "test", 1, func(cfg *ProducerConfig) {
			cfg.FlushInterval = 50 * time.Millisecond
		},
	)

	require.NoError(t, producer.Send([]byte("key"), []byte("value")))
	time.Sleep(150 * time.Millisecond)

	consumer := newTestConsumer(t, addr, "autoflush-group")
	ch, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 1, 2*time.Second)
	require.NoError(t, err)
	assert.Len(t, messages, 1)
}

func TestSubscribeAlreadySubscribed(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "test", 1)

	consumer := newTestConsumer(t, addr, "dup-group")
	_, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	_, err = consumer.SubscribeChan("test")
	assert.Error(t, err)
	assert.Equal(t, `already subscribed to topic "test"`, err.Error())
}

func TestUnsubscribeClosesChannel(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "test", 1)

	consumer := newTestConsumer(t, addr, "unsub-group")
	ch, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	require.NoError(t, consumer.Unsubscribe("test"))

	msg, ok := <-ch
	assert.False(t, ok, "channel should be closed")
	assert.Nil(t, msg)
}

func TestUnsubscribeNotSubscribed(t *testing.T) {
	addr := startIntegrationServer(t)

	consumer := newTestConsumer(t, addr, "unsub-group")
	err := consumer.Unsubscribe("nope")
	assert.Error(t, err)
}

func TestUnsubscribeAndResubscribeSucceeds(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "test", 1)

	consumer := newTestConsumer(t, addr, "resub-group2")

	_, err := consumer.SubscribeChan("test")
	require.NoError(t, err)
	require.NoError(t, consumer.Unsubscribe("test"))
	_, err = consumer.SubscribeChan("test")
	require.NoError(t, err)
}

func TestConsumerCloseAllChannels(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "t1", 1)
	createTopic(t, addr, "t2", 1)

	consumer := newTestConsumer(t, addr, "close-group")

	ch1, err := consumer.SubscribeChan("t1")
	require.NoError(t, err)
	ch2, err := consumer.SubscribeChan("t2")
	require.NoError(t, err)

	require.NoError(t, consumer.Close())

	_, ok1 := <-ch1
	assert.False(t, ok1, "ch1 should be closed")
	_, ok2 := <-ch2
	assert.False(t, ok2, "ch2 should be closed")
}

func TestConsumerCloseNoSubscriptions(t *testing.T) {
	addr := startIntegrationServer(t)

	consumer := newTestConsumer(t, addr, "empty-group")
	assert.NoError(t, consumer.Close())
}

func TestMultiPartitionConsume(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestTopicProducer(t, addr, "multi", 3)

	keys := []string{"key-a", "key-b", "key-c"}
	for i := 0; i < 6; i++ {
		key := keys[i%3]
		value := fmt.Sprintf("msg-%d", i)
		require.NoError(t, producer.Send([]byte(key), []byte(value)))
	}
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr, "multi-group")
	ch, err := consumer.SubscribeChan("multi")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 6, 2*time.Second)
	require.NoError(t, err)

	partitionCounts := make(map[int32]int)
	for _, msg := range messages {
		partitionCounts[msg.Partition]++
		assert.GreaterOrEqual(t, msg.Partition, int32(0))
		assert.Less(t, msg.Partition, int32(3))
	}

	assert.Len(t, partitionCounts, 3, "messages should come from all 3 partitions")
}

func TestConcurrentIndependentConsumers(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestTopicProducer(t, addr, "test", 1)
	for i := 0; i < 10; i++ {
		require.NoError(t, producer.Send(nil, []byte(fmt.Sprintf("msg-%d", i))))
	}
	require.NoError(t, producer.Flush())

	numConsumers := 3
	var wg sync.WaitGroup
	var totalReceived atomic.Int64

	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			consumer := newTestConsumer(
				t, addr, fmt.Sprintf("independent-group-%d", id),
			)
			ch, err := consumer.SubscribeChan("test")
			require.NoError(t, err)

			messages, err := collectMessages(ch, 10, 3*time.Second)
			require.NoError(t, err)

			totalReceived.Add(int64(len(messages)))
		}(i)
	}

	wg.Wait()

	expected := int64(numConsumers * 10)
	assert.Equal(t, expected, totalReceived.Load(), "each consumer should receive all messages")
}

func TestConsumerGroupSplitsMessages(t *testing.T) {
	addr := startIntegrationServer(t)

	// create a topic with 2 partitions
	createTopic(t, addr, "group-test", 2)

	// start two consumers in the same group
	consumer1 := newTestConsumer(t, addr, "shared-group")
	ch1, err := consumer1.SubscribeChan("group-test")
	require.NoError(t, err)

	// delay to let consumer1 join first
	time.Sleep(200 * time.Millisecond)

	consumer2 := newTestConsumer(t, addr, "shared-group")
	ch2, err := consumer2.SubscribeChan("group-test")
	require.NoError(t, err)

	// wait for rebalance to complete
	time.Sleep(500 * time.Millisecond)

	// produce messages - round-robin across the 2 partitions
	producer := newTestTopicProducer(t, addr, "group-test", 2)
	for i := 0; i < 10; i++ {
		require.NoError(t, producer.Send(nil, []byte(fmt.Sprintf("msg-%d", i))))
	}
	require.NoError(t, producer.Flush())

	// collect from both consumers
	var mu sync.Mutex
	var all []*Message

	var wg sync.WaitGroup
	collect := func(ch <-chan *Message) {
		defer wg.Done()
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		for {
			select {
			case msg := <-ch:
				if msg == nil {
					return
				}
				mu.Lock()
				all = append(all, msg)
				mu.Unlock()
			case <-timer.C:
				return
			}
		}
	}

	wg.Add(2)
	go collect(ch1)
	go collect(ch2)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, 10, len(all), "group consumers should collectively receive all messages")
}
