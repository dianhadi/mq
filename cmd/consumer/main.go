package main

import (
	"log"

	handler "github.com/dianhadi/mq/internal/handler/mq"
	"github.com/dianhadi/mq/pkg/mq"
	mqKafka "github.com/dianhadi/mq/pkg/mq/kafka"
	mqRabbit "github.com/dianhadi/mq/pkg/mq/rabbitmq"
)

func main() {
	broker := "localhost:9092"

	host := "localhost"
	port := 5672
	username := "admin"
	password := "admin"

	// Init Configuration
	configKafka := mq.Config{
		Hosts: []string{broker},
		Group: "golang-group",
	}

	configRabbit := mq.Config{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
	}

	// Init Consumer
	consumerKafka, err := mqKafka.NewConsumer(configKafka)
	if err != nil {
		panic(err)
	}
	// defer consumerKafka.Close()
	consumerRabbitMq, err := mqRabbit.NewConsumer(configRabbit)
	if err != nil {
		panic(err)
	}
	defer consumerRabbitMq.Close()

	log.Println("Init Handler")
	handlerKafka, err := handler.New()
	handlerRabbitMq, err := handler.New()

	log.Println("Add Handler Consumer")
	cfgConsumer := mq.ConsumerConfig{}
	if err != nil {
		panic(err)
	}
	consumerKafka.AddConsumer("test-topic-1", cfgConsumer, handlerKafka.Consume)
	consumerKafka.AddConsumer("test-topic-2", cfgConsumer, handlerKafka.Consume)

	consumerRabbitMq.AddConsumer("test-topic-3", cfgConsumer, handlerRabbitMq.Consume)
	consumerRabbitMq.AddConsumerWithChannel("test-topic-4", "channel-1", cfgConsumer, handlerRabbitMq.Consume)

	log.Println("Start")
	consumerRabbitMq.Start()
	// consumerKafka.Start()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic: %v", r)
		}
	}()
}
