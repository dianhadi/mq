package rabbitmq

import (
	"time"

	"github.com/streadway/amqp"
)

type RabbitMqMessage struct {
	message *amqp.Delivery
}

func (m RabbitMqMessage) Body() []byte {
	return m.message.Body
}

func (m RabbitMqMessage) Time() time.Time {
	return m.message.Timestamp
}

func (m RabbitMqMessage) Requeue() error {
	return m.message.Nack(false, true)
}

func (m RabbitMqMessage) Done() error {
	return m.message.Ack(false)
}
