package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dianhadi/mq/pkg/mq"
	"github.com/streadway/amqp"
)

type RabbitMqConsumer struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	queue    []amqp.Queue
	handlers map[string]mq.HandlerFunc
}

func NewConsumer(config mq.Config) (*RabbitMqConsumer, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", config.Username, config.Password, config.Host, config.Port)

	// Establish a connection to RabbitMQ server
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	rabbitMqConsumer := RabbitMqConsumer{
		conn:     conn,
		channel:  channel,
		queue:    []amqp.Queue{},
		handlers: make(map[string]mq.HandlerFunc),
	}
	return &rabbitMqConsumer, nil
}

func (c *RabbitMqConsumer) AddConsumer(queue string, cfg mq.ConsumerConfig, handler mq.HandlerFunc) error {
	q, err := c.channel.QueueDeclare(
		queue, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}

	c.queue = append(c.queue, q)
	c.handlers[queue] = handler

	return nil
}

func (c *RabbitMqConsumer) AddConsumerWithChannel(exchange, queue string, cfg mq.ConsumerConfig, handler mq.HandlerFunc) error {
	err := c.channel.ExchangeDeclare(
		exchange, // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return err
	}

	q, err := c.channel.QueueDeclare(
		queue, // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}

	err = c.channel.QueueBind(
		q.Name,   // queue name
		"",       // routing key
		exchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}
	c.queue = append(c.queue, q)
	c.handlers[queue] = handler

	return nil
}

func (c *RabbitMqConsumer) Start() error {
	if len(c.queue) == 0 {
		return errors.New("No topic is added")
	}

	// Create a wait group to wait for goroutines to finish
	var wg sync.WaitGroup

	for _, q := range c.queue {
		msgs, err := c.channel.Consume(
			q.Name, // queue
			"",     // consumer
			false,  // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		if err != nil {
			return err
		}

		// Increment the wait group for each goroutine
		wg.Add(1)

		go func() {
			defer wg.Done() // Decrement the wait group when done

			for msg := range msgs {
				handler := c.handlers[q.Name]
				ctx := context.Background()
				message := RabbitMqMessage{Delivery: &msg}
				handler(ctx, &message)
			}
		}()

	}

	// Wait for all goroutines to finish
	wg.Wait()

	return nil
}

func (k *RabbitMqConsumer) Close() {
	k.conn.Close()
	k.channel.Close()
}
