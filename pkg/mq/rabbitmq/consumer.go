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
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      []amqp.Queue
	handlers   map[string]mq.HandlerFunc
	configs    map[string]mq.ConsumerConfig
	wg         sync.WaitGroup
}

func NewConsumer(config mq.Config) (*RabbitMqConsumer, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", config.RabbitMq.Username, config.RabbitMq.Password, config.RabbitMq.Host, config.RabbitMq.Port)

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
		connection: conn,
		channel:    channel,
		queue:      []amqp.Queue{},
		handlers:   make(map[string]mq.HandlerFunc),
		configs:    make(map[string]mq.ConsumerConfig),
		wg:         sync.WaitGroup{},
	}
	return &rabbitMqConsumer, nil
}

func (c *RabbitMqConsumer) AddHandler(queue string, cfg mq.ConsumerConfig, handler mq.HandlerFunc) error {
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
	c.configs[queue] = cfg

	return nil
}

func (c *RabbitMqConsumer) AddHandlerWithChannel(exchange, queue string, cfg mq.ConsumerConfig, handler mq.HandlerFunc) error {
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
	c.configs[queue] = cfg

	return nil
}

func (c *RabbitMqConsumer) Start() error {
	if len(c.queue) == 0 {
		return errors.New("No topic is added")
	}

	for _, q := range c.queue {
		queueName := q.Name
		config, ok := c.configs[queueName]
		if !ok {
			return errors.New("config not found")
		}

		msgs, err := c.channel.Consume(
			q.Name,                    // queue
			"",                        // consumer
			config.RabbitMq.AutoAck,   // auto-ack
			config.RabbitMq.Exclusive, // exclusive
			config.RabbitMq.NoLocal,   // no-local
			config.RabbitMq.NoWait,    // no-wait
			nil,                       // args
		)
		if err != nil {
			return err
		}

		// Increment the wait group for each goroutine
		c.wg.Add(1)

		go func() {
			defer c.wg.Done() // Decrement the wait group when done

			for msg := range msgs {
				handler := c.handlers[q.Name]
				ctx := context.Background()
				message := RabbitMqMessage{message: &msg}
				handler(ctx, &message)
			}
		}()

	}

	// Wait for all goroutines to finish
	c.wg.Wait()

	return nil
}

func (c *RabbitMqConsumer) Close() {
	c.connection.Close()
	c.channel.Close()
	c.wg.Wait()
}
