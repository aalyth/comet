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
	createTopic(t, addr, "test", 1)

	producer := newTestProducer(t, addr)
	require.NoError(t, producer.Send("test", []byte("k1"), []byte("v1")))
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr)
	ch, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 1, 2*time.Second)
	require.NoError(t, err)

	assertMessage(t, messages[0], Expect("k1", "v1", 0, "test"))
}

func TestMultipleMessagesInOrder(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "ordered", 1)

	producer := newTestProducer(t, addr)
	var expected []MessageExpectation
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		value := fmt.Sprintf("value-%d", i)
		require.NoError(t, producer.Send("ordered", []byte(key), []byte(value)))
		expected = append(expected, Expect(key, value, int64(i), "ordered"))
	}
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr)
	ch, err := consumer.SubscribeChan("ordered")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 10, 2*time.Second)
	require.NoError(t, err)

	assertMessages(t, messages, expected)
}

func TestMultiplePartitions(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "spread", 4)

	producer := newTestProducer(t, addr)
	for i := 0; i < 20; i++ {
		value := []byte(fmt.Sprintf("msg-%d", i))
		require.NoError(t, producer.Send("spread", nil, value))
	}
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr)
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
	createTopic(t, addr, "keyed", 4)

	producer := newTestProducer(t, addr)
	for i := 0; i < 10; i++ {
		require.NoError(
			t,
			producer.Send(
				"keyed", []byte("same-key"), []byte(fmt.Sprintf("msg-%d", i)),
			),
		)
	}
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr)
	ch, err := consumer.SubscribeChan("keyed")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 10, 2*time.Second)
	require.NoError(t, err)

	assertAllMessagesFromSamePartition(t, messages)
}

func TestSubscribeWithHandler(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "test", 1)

	producer := newTestProducer(t, addr)
	var expected []MessageExpectation
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("v%d", i)
		require.NoError(t, producer.Send("test", []byte(key), []byte(value)))
		expected = append(expected, Expect(key, value, int64(i), "test"))
	}
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr)

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
	createTopic(t, addr, "test", 1)

	producer := newTestProducer(t, addr)

	// Produce first batch
	for i := 0; i < 2; i++ {
		require.NoError(t, producer.Send("test", nil, []byte(fmt.Sprintf("batch1-%d", i))))
	}
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr)
	ch, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 2, 2*time.Second)
	require.NoError(t, err)
	assert.Len(t, messages, 2)

	require.NoError(t, consumer.Unsubscribe("test"))

	// Produce second batch
	for i := 0; i < 2; i++ {
		require.NoError(t, producer.Send("test", nil, []byte(fmt.Sprintf("batch2-%d", i))))
	}
	require.NoError(t, producer.Flush())

	// Resubscribe (should get all 4 messages since InitialOffset=0)
	ch2, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	allMessages, err := collectMessages(ch2, 4, 2*time.Second)
	require.NoError(t, err)
	assert.Len(t, allMessages, 4)
}

func TestProducerCloseFlushesBuffered(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "test", 1)

	producer := newTestProducer(
		t, addr, func(cfg *ProducerConfig) {
			cfg.FlushInterval = 10 * time.Second
		},
	)

	for i := 0; i < 5; i++ {
		require.NoError(t, producer.Send("test", nil, []byte(fmt.Sprintf("msg-%d", i))))
	}

	require.NoError(t, producer.Close())

	consumer := newTestConsumer(t, addr)
	ch, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 5, 2*time.Second)
	require.NoError(t, err)
	assert.Len(t, messages, 5)
}

func TestProduceNonExistentTopic(t *testing.T) {
	addr := startIntegrationServer(t)

	producer := newTestProducer(t, addr)
	require.NoError(t, producer.Send("ghost", []byte("key"), []byte("value")))
	assert.Error(t, producer.Flush())
}

func TestSubscribeNonExistentTopic(t *testing.T) {
	addr := startIntegrationServer(t)

	consumer := newTestConsumer(t, addr)
	_, err := consumer.SubscribeChan("ghost")
	assert.Error(t, err)
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

			producer := newTestProducer(t, addr)
			for j := 0; j < messagesPerProducer; j++ {
				value := []byte(fmt.Sprintf("producer-%d-msg-%d", producerID, j))
				require.NoError(t, producer.Send("concurrent", nil, value))
			}
			require.NoError(t, producer.Flush())
		}(i)
	}

	wg.Wait()

	consumer := newTestConsumer(t, addr)
	ch, err := consumer.SubscribeChan("concurrent")
	require.NoError(t, err)

	expectedTotal := numProducers * messagesPerProducer
	messages, err := collectMessages(ch, expectedTotal, 5*time.Second)
	require.NoError(t, err)
	assert.Len(t, messages, expectedTotal)
}

func TestProducerBuffering(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "test", 1)

	producer := newTestProducer(
		t, addr, func(cfg *ProducerConfig) {
			cfg.FlushInterval = 10 * time.Second
		},
	)

	for i := 0; i < 5; i++ {
		require.NoError(t, producer.Send("test", []byte("key"), []byte("value")))
	}

	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr)
	ch, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 5, 2*time.Second)
	require.NoError(t, err)
	assert.Len(t, messages, 5)
}

func TestSendAfterClose(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "test", 1)

	producer := newTestProducer(t, addr)
	require.NoError(t, producer.Close())

	err := producer.Send("test", []byte("key"), []byte("value"))
	assert.Error(t, err)
}

func TestFlushEmptyBuffer(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "test", 1)

	producer := newTestProducer(t, addr)
	assert.NoError(t, producer.Flush())
}

func TestAutoFlush(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "test", 1)

	producer := newTestProducer(
		t, addr, func(cfg *ProducerConfig) {
			cfg.FlushInterval = 50 * time.Millisecond
		},
	)

	require.NoError(t, producer.Send("test", []byte("key"), []byte("value")))
	time.Sleep(150 * time.Millisecond)

	consumer := newTestConsumer(t, addr)
	ch, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	messages, err := collectMessages(ch, 1, 2*time.Second)
	require.NoError(t, err)
	assert.Len(t, messages, 1)
}

func TestSubscribeAlreadySubscribed(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "test", 1)

	consumer := newTestConsumer(t, addr)
	_, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	_, err = consumer.SubscribeChan("test")
	assert.Error(t, err)
	assert.Equal(t, `already subscribed to topic "test"`, err.Error())
}

func TestUnsubscribeClosesChannel(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "test", 1)

	consumer := newTestConsumer(t, addr)
	ch, err := consumer.SubscribeChan("test")
	require.NoError(t, err)

	require.NoError(t, consumer.Unsubscribe("test"))

	msg, ok := <-ch
	assert.False(t, ok, "channel should be closed")
	assert.Nil(t, msg)
}

func TestUnsubscribeNotSubscribed(t *testing.T) {
	addr := startIntegrationServer(t)

	consumer := newTestConsumer(t, addr)
	err := consumer.Unsubscribe("nope")
	assert.Error(t, err)
}

func TestUnsubscribeAndResubscribeSucceeds(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "test", 1)

	consumer := newTestConsumer(t, addr)

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

	consumer := newTestConsumer(t, addr)

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

	consumer := newTestConsumer(t, addr)
	assert.NoError(t, consumer.Close())
}

func TestMultiPartitionConsume(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "multi", 3)

	producer := newTestProducer(t, addr)

	keys := []string{"key-a", "key-b", "key-c"}
	for i := 0; i < 6; i++ {
		key := keys[i%3]
		value := fmt.Sprintf("msg-%d", i)
		require.NoError(t, producer.Send("multi", []byte(key), []byte(value)))
	}
	require.NoError(t, producer.Flush())

	consumer := newTestConsumer(t, addr)
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

func TestConcurrentConsumers(t *testing.T) {
	addr := startIntegrationServer(t)
	createTopic(t, addr, "test", 1)

	producer := newTestProducer(t, addr)
	for i := 0; i < 10; i++ {
		require.NoError(t, producer.Send("test", nil, []byte(fmt.Sprintf("msg-%d", i))))
	}
	require.NoError(t, producer.Flush())

	numConsumers := 3
	var wg sync.WaitGroup
	var totalReceived atomic.Int64

	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			consumer := newTestConsumer(t, addr)
			ch, err := consumer.SubscribeChan("test")
			require.NoError(t, err)

			messages, err := collectMessages(ch, 10, 2*time.Second)
			require.NoError(t, err)

			totalReceived.Add(int64(len(messages)))
		}()
	}

	wg.Wait()

	expected := int64(numConsumers * 10)
	assert.Equal(t, expected, totalReceived.Load(), "each consumer should receive all messages")
}
