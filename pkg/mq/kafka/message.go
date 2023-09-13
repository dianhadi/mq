package kafka

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaMessage struct {
	*kafka.Message
}

func (k KafkaMessage) Body() []byte {
	return k.Value
}

func (k KafkaMessage) Time() time.Time {
	return k.Timestamp
}
