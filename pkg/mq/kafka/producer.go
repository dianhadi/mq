package kafka

import (
	"log"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dianhadi/mq/pkg/mq"
)

type KafkaProducer struct {
	*kafka.Producer
}

func NewProducer(config mq.Config) (*KafkaProducer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Hosts, ","),
	})
	if err != nil {
		log.Printf("Error creating Kafka producer: %v", err)
	}

	kafkaProducer := KafkaProducer{Producer: producer}
	return &kafkaProducer, nil
}

func (k KafkaProducer) SendMessage(topic, msg string) error {
	err := k.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
	}, nil)

	return err
}

func (k KafkaProducer) Close() {
	k.Close()
}
