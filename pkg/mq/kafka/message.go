package kafka

import (
	"errors"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaMessage struct {
	message *kafka.Message
}

func (m KafkaMessage) Body() []byte {
	return m.message.Value
}

func (m KafkaMessage) Time() time.Time {
	return m.message.Timestamp
}

func (m KafkaMessage) Requeue() error {
	return errors.New("Requeue is not implemented in Kafka")
}

func (m KafkaMessage) Done() error {
	return nil
}
