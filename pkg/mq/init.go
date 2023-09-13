package mq

import (
	"context"
	"time"
)

type Config struct {
	Host  string // temp
	Port  int    // temp
	Hosts []string
	Group string
}

type ConsumerConfig struct {
}

type Producer interface {
	// SendMessage sends a message to the specified topic.
	SendMessage(topic, msg string) error
	// Close closes the MQ producer.
	Close()
}

type Consumer interface {
	// AddConsumer adds a consumer for the specified topic.
	// It takes a ConsumerConfig for configuring the consumer and a HandlerFunc for processing messages.
	AddConsumer(topic string, cfg ConsumerConfig, handler HandlerFunc) error
	// AddConsumerWithChannel adds a consumer for the specified topic and channel.
	// It takes a ConsumerConfig for configuring the consumer and a HandlerFunc for processing messages.
	AddConsumerWithChannel(topic, channel string, cfg ConsumerConfig, handler HandlerFunc) error
	// Start starts the MQ consumer to begin consuming messages.
	Start() error
	// Close closes the MQ consumer.
	Close()
}

type Message interface {
	// Body returns the body of the message as a byte slice.
	Body() []byte
	// Time returns the time when the message was received.
	Time() time.Time
}

type HandlerFunc func(ctx context.Context, msg Message) error
