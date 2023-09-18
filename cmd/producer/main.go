package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	handler "github.com/dianhadi/mq/internal/handler/http"
	"github.com/dianhadi/mq/pkg/mq"
	mqKafka "github.com/dianhadi/mq/pkg/mq/kafka"
	mqNsq "github.com/dianhadi/mq/pkg/mq/nsq"
	mqRabbit "github.com/dianhadi/mq/pkg/mq/rabbitmq"
	"github.com/go-chi/chi"
)

func main() {
	broker := "localhost:9092"
	host := "localhost"
	port := 5672
	username := "admin"
	password := "admin"
	serverPort := "8008"
	portD := 4150

	// Init Configuration
	configKafka := mq.ConfigKafka{
		Hosts: []string{broker},
	}
	configRabbit := mq.ConfigRabbitMq{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
	}
	configNsq := mq.ConfigNsq{
		ProducerHost: host,
		ProducerPort: portD,
	}
	config := mq.Config{
		Kafka:    configKafka,
		RabbitMq: configRabbit,
		Nsq:      configNsq,
	}

	// Init Producer
	producerKafka, err := mqKafka.NewProducer(config)
	if err != nil {
		panic(err)
	}
	defer producerKafka.Close()
	producerRabbitMq, err := mqRabbit.NewProducer(config)
	if err != nil {
		panic(err)
	}
	defer producerRabbitMq.Close()
	producerNsq, err := mqNsq.NewProducer(config)
	if err != nil {
		panic(err)
	}
	defer producerNsq.Close()

	log.Println("Init Handler")
	handlerKafka, err := handler.New(producerKafka)
	if err != nil {
		panic(err)
	}
	handlerRabbitMq, err := handler.New(producerRabbitMq)
	if err != nil {
		panic(err)
	}
	handlerNsq, err := handler.New(producerNsq)
	if err != nil {
		panic(err)
	}

	r := chi.NewRouter()

	log.Println("Register Route")
	r.Post("/kafka", handlerKafka.Publish)
	r.Post("/rabbit", handlerRabbitMq.Publish)
	r.Post("/nsq", handlerNsq.Publish)

	log.Printf("Starting server on port %s...", serverPort)
	startServer(":"+serverPort, r)
}

func startServer(port string, r http.Handler) {
	srv := http.Server{
		Addr:    port,
		Handler: r,
	}

	// Create a channel that listens on incomming interrupt signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(
		signalChan,
		syscall.SIGHUP,  // kill -SIGHUP XXXX
		syscall.SIGINT,  // kill -SIGINT XXXX or Ctrl+c
		syscall.SIGQUIT, // kill -SIGQUIT XXXX
	)

	// Graceful shutdown
	go func() {
		// Wait for a new signal on channel
		<-signalChan
		// Signal received, shutdown the server
		log.Println("shutting down..")

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		srv.Shutdown(ctx)

		// Check if context timeouts, in worst case call cancel via defer
		select {
		case <-time.After(21 * time.Second):
			log.Println("not all connections done")
		case <-ctx.Done():
		}
	}()

	err := srv.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
