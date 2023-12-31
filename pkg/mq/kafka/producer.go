package kafka

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dianhadi/mq/pkg/mq"
)

type KafkaProducer struct {
	producer *kafka.Producer
}

func NewProducer(config mq.Config) (*KafkaProducer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Kafka.Hosts, ","),
	})
	if err != nil {
		return nil, err
	}

	kafkaProducer := KafkaProducer{
		producer: producer,
	}
	return &kafkaProducer, nil
}

func (p KafkaProducer) SendDirectMessage(topic, msg string) error {
	return p.SendMessage(topic, msg)
}

func (p KafkaProducer) SendMessage(topic, msg string) error {
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
	}, nil)

	return err
}

func (p KafkaProducer) Close() {
	p.producer.Close()
}
