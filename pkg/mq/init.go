package mq

import (
	"context"
	"time"
)

type Config struct {
	// For RabbitMQ
	Host     string
	Port     int
	Username string
	Password string
	// For RabbitMQ
	Hosts []string
	Group string
}

type ConsumerConfig struct {
}

type Producer interface {
	// SendMessage sends a message to the specified topic to direct queue (only on Rabbit MQ).
	SendDirectMessage(topic, msg string) error
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
	// Requeue send message back to queue.
	Requeue() error
	// Done send message to queue that a message is done
	Done() error
}

type HandlerFunc func(ctx context.Context, msg Message) error
