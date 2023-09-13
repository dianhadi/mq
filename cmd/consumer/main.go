package main

import (
	"log"

	handler "github.com/dianhadi/mq/internal/handler/mq"
	"github.com/dianhadi/mq/pkg/mq"
	mqKafka "github.com/dianhadi/mq/pkg/mq/kafka"
)

func main() {
	broker := "localhost:9092"

	// Init Configuration
	config := mq.Config{
		Hosts: []string{broker},
		Group: "golang-group",
	}

	// Init Consumer
	consumer, err := mqKafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	log.Println("Init Handler")
	handlerKafka, err := handler.New()
	cfgConsumer := mq.ConsumerConfig{}
	if err != nil {
		panic(err)
	}

	log.Println("Add Handler Consumer")
	consumer.AddConsumer("test-topic-1", cfgConsumer, handlerKafka.Consume)
	consumer.AddConsumer("test-topic-2", cfgConsumer, handlerKafka.Consume)

	log.Println("Start")
	consumer.Start()
}
