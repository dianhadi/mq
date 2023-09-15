package kafka

import (
	"errors"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaMessage struct {
	*kafka.Message
}

func (m KafkaMessage) Body() []byte {
	return m.Message.Value
}

func (m KafkaMessage) Time() time.Time {
	return m.Message.Timestamp
}

func (m KafkaMessage) Requeue() error {
	return errors.New("Requeue is not implemented in Kafka")
}

func (m KafkaMessage) Done() error {
	return nil
}
