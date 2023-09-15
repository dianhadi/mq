package rabbitmq

import (
	"time"

	"github.com/streadway/amqp"
)

type RabbitMqMessage struct {
	*amqp.Delivery
}

func (m RabbitMqMessage) Body() []byte {
	return m.Delivery.Body
}

func (m RabbitMqMessage) Time() time.Time {
	return m.Delivery.Timestamp
}

func (m RabbitMqMessage) Requeue() error {
	return m.Delivery.Nack(false, true)
}

func (m RabbitMqMessage) Done() error {
	return m.Delivery.Ack(false)
}
