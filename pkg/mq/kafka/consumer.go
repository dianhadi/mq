package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dianhadi/mq/pkg/mq"
)

type KafkaConsumer struct {
	consumer *kafka.Consumer
	topics   []string
	handlers map[string]mq.HandlerFunc
}

func NewConsumer(config mq.Config) (*KafkaConsumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Kafka.Hosts, ","),
		"group.id":          config.Kafka.Group,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		return nil, err
	}

	kafkaConsumer := KafkaConsumer{
		consumer: consumer,
		topics:   []string{},
		handlers: make(map[string]mq.HandlerFunc),
	}
	return &kafkaConsumer, nil
}

func (c *KafkaConsumer) AddHandler(topic string, cfg mq.ConsumerConfig, handler mq.HandlerFunc) error {
	c.topics = append(c.topics, topic)
	c.handlers[topic] = handler

	return nil
}

func (c *KafkaConsumer) AddHandlerWithChannel(topic, channel string, cfg mq.ConsumerConfig, handler mq.HandlerFunc) error {
	return errors.New("not implemented in kafka")
}

func (c *KafkaConsumer) Start() error {
	if len(c.topics) == 0 {
		return errors.New("No topic is added")
	}

	c.consumer.SubscribeTopics(c.topics, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.consumer.ReadMessage(time.Second)
		if err == nil {
			handler := c.handlers[*msg.TopicPartition.Topic]
			ctx := context.Background()
			message := KafkaMessage{message: msg}
			handler(ctx, message)
		} else if err.Error() != kafka.ErrTimedOut.String() {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
	return nil
}

func (k *KafkaConsumer) Close() {
	k.consumer.Close()
}
