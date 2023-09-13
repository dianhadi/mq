package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dianhadi/mq/pkg/mq"
)

type KafkaConsumer struct {
	*kafka.Consumer
	topics   []string
	handlers map[string]mq.HandlerFunc
}

func NewConsumer(config mq.Config) (*KafkaConsumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Hosts, ","),
		"group.id":          config.Group,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		log.Printf("Error creating Kafka consumer: %v", err)
	}

	kafkaConsumer := KafkaConsumer{
		Consumer: consumer,
		topics:   []string{},
		handlers: make(map[string]mq.HandlerFunc),
	}
	return &kafkaConsumer, nil
}

func (k *KafkaConsumer) AddConsumer(topic string, cfg mq.ConsumerConfig, handler mq.HandlerFunc) error {
	k.topics = append(k.topics, topic)
	k.handlers[topic] = handler

	return nil
}

func (k *KafkaConsumer) AddConsumerWithChannel(topic, channel string, cfg mq.ConsumerConfig, handler mq.HandlerFunc) error {
	return errors.New("not implemented in kafka")
}

func (k *KafkaConsumer) Start() error {
	if len(k.topics) == 0 {
		return errors.New("No topic is added")
	}

	k.SubscribeTopics(k.topics, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := k.ReadMessage(time.Second)
		if err == nil {
			handler := k.handlers[*msg.TopicPartition.Topic]
			ctx := context.Background()
			message := KafkaMessage{Message: msg}
			handler(ctx, message)
		} else if err.Error() != kafka.ErrTimedOut.String() {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
	return nil
}

func (k *KafkaConsumer) Close() {
	k.Close()
}
