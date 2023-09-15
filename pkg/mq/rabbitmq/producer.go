package rabbitmq

import (
	"fmt"
	"time"

	"github.com/dianhadi/mq/pkg/mq"
	"github.com/streadway/amqp"
)

type RabbitMqProducer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewProducer(config mq.Config) (*RabbitMqProducer, error) {
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

	rabbitMqProducer := RabbitMqProducer{
		conn:    conn,
		channel: channel,
	}
	return &rabbitMqProducer, nil
}

func (p RabbitMqProducer) SendDirectMessage(queue, msg string) error {
	// Declare the queue
	q, err := p.channel.QueueDeclare(
		queue, // name
		true,  // Durable
		false, // Auto-deleted
		false, // Internal
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		return err
	}

	timestamp := time.Now()

	// Publish the message to the queue
	err = p.channel.Publish(
		"",     // Exchange
		q.Name, // Routing key
		false,  // Mandatory
		false,  // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
			Timestamp:   timestamp,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (p RabbitMqProducer) SendMessage(exchange, msg string) error {
	// Declare the fanout exchange
	err := p.channel.ExchangeDeclare(
		exchange, // Exchange name
		"fanout", // Exchange type
		true,     // Durable
		false,    // Auto-deleted
		false,    // Internal
		false,    // No-wait
		nil,      // Arguments
	)
	if err != nil {
		return err
	}

	timestamp := time.Now()

	// Publish the message to the fanout exchange (no routing key needed)
	err = p.channel.Publish(
		exchange, // Exchange
		"",       // Routing key (empty for fanout exchange)
		false,    // Mandatory
		false,    // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
			Timestamp:   timestamp,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (p RabbitMqProducer) Close() {
	p.conn.Close()
	p.channel.Close()
}
