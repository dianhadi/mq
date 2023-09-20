package nsq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dianhadi/mq/pkg/mq"
	"github.com/nsqio/go-nsq"
)

type NsqConsumer struct {
	lookupD string
	cons    []*nsq.Consumer
	wg      sync.WaitGroup
}

// NewConsumer creates a new NSQ consumer.
func NewConsumer(config mq.Config) (*NsqConsumer, error) {
	url := fmt.Sprintf("%s:%d", config.Nsq.ConsumerHost, config.Nsq.ConsumerPort)
	return &NsqConsumer{
		lookupD: url,
		cons:    []*nsq.Consumer{},
		wg:      sync.WaitGroup{},
	}, nil
}

func (c *NsqConsumer) AddHandler(topic string, cfg mq.ConsumerConfig, handler mq.HandlerFunc) error {
	return errors.New("not implemented in nsq")
}

func (c *NsqConsumer) AddHandlerWithChannel(topic, channel string, cfg mq.ConsumerConfig, handler mq.HandlerFunc) error {
	config := nsq.NewConfig()
	config.MaxAttempts = cfg.Nsq.MaxAttempts
	config.MaxInFlight = cfg.Nsq.MaxInFlight

	// create the consumer
	con, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		return err
	}

	con.AddConcurrentHandlers(nsq.HandlerFunc(func(msg *nsq.Message) error {
		// Create a Message instance to wrap the NSQ message.
		ctx := context.Background()
		message := NsqMessage{message: msg}
		return handler(ctx, &message)
	}), cfg.Nsq.Concurrency)

	c.cons = append(c.cons, con)

	return nil
}

func (c *NsqConsumer) Start() error {
	if len(c.cons) == 0 {
		return errors.New("No topic is added")
	}

	for _, con := range c.cons {
		// Increment the wait group for each goroutine
		c.wg.Add(1)

		err := con.ConnectToNSQLookupd(c.lookupD)
		if err != nil {
			return err
		}
	}

	c.wg.Wait()

	return nil
}

func (c *NsqConsumer) Close() {
	for _, con := range c.cons {
		con.Stop()
		c.wg.Done()
	}
}
