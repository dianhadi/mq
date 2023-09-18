package main

import (
	"log"

	handler "github.com/dianhadi/mq/internal/handler/mq"
	"github.com/dianhadi/mq/pkg/mq"
	mqKafka "github.com/dianhadi/mq/pkg/mq/kafka"
	mqNsq "github.com/dianhadi/mq/pkg/mq/nsq"
	mqRabbit "github.com/dianhadi/mq/pkg/mq/rabbitmq"
)

func main() {
	broker := "localhost:9092"
	group := "golang-group"
	host := "localhost"
	port := 5672
	username := "admin"
	password := "admin"
	portLookupD := 4161

	// Init Configuration
	configKafka := mq.ConfigKafka{
		Hosts: []string{broker},
		Group: group,
	}
	configRabbit := mq.ConfigRabbitMq{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
	}
	configNsq := mq.ConfigNsq{
		ConsumerHost: host,
		ConsumerPort: portLookupD,
	}
	config := mq.Config{
		Kafka:    configKafka,
		RabbitMq: configRabbit,
		Nsq:      configNsq,
	}

	// Init Consumer
	consumerKafka, err := mqKafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	// defer consumerKafka.Close()
	consumerRabbitMq, err := mqRabbit.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	defer consumerRabbitMq.Close()
	consumerNsq, err := mqNsq.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	// defer consumerNsq.Close()

	log.Println("Init Handler")
	handlerKafka, err := handler.New()
	handlerRabbitMq, err := handler.New()
	handlerNsq, err := handler.New()

	log.Println("Add Handler Consumer")
	nsqConsumerConfig := mq.ConsumerConfigNsq{
		MaxInFlight: 1,
		MaxAttempts: 100,
		Concurrency: 10,
	}
	cfgConsumer := mq.ConsumerConfig{
		Nsq: nsqConsumerConfig,
	}
	if err != nil {
		panic(err)
	}
	consumerKafka.AddHandler("test-topic-1", cfgConsumer, handlerKafka.Consume)
	consumerKafka.AddHandler("test-topic-2", cfgConsumer, handlerKafka.Consume)

	consumerRabbitMq.AddHandler("test-topic-3", cfgConsumer, handlerRabbitMq.Consume)
	consumerRabbitMq.AddHandlerWithChannel("test-topic-4", "channel-1", cfgConsumer, handlerRabbitMq.Consume)

	consumerNsq.AddHandlerWithChannel("test-topic-5", "channel-1", cfgConsumer, handlerNsq.Consume)

	log.Println("Start")
	consumerRabbitMq.Start()
	// consumerKafka.Start()
	// consumerNsq.Start()
}
