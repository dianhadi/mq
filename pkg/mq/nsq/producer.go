package nsq

import (
	"fmt"

	"github.com/dianhadi/mq/pkg/mq"
	"github.com/nsqio/go-nsq"
)

type NsqProducer struct {
	producer *nsq.Producer
}

func NewProducer(config mq.Config) (*NsqProducer, error) {
	cfg := nsq.NewConfig()
	url := fmt.Sprintf("%s:%d", config.Nsq.ProducerHost, config.Nsq.ProducerPort)
	producer, err := nsq.NewProducer(url, cfg)
	if err != nil {
		return nil, err
	}
	return &NsqProducer{producer: producer}, nil
}

func (p NsqProducer) SendDirectMessage(topic, msg string) error {
	return p.SendMessage(topic, msg)
}

func (p NsqProducer) SendMessage(topic, msg string) error {
	return p.producer.Publish(topic, []byte(msg))
}

func (p NsqProducer) Close() {
	p.producer.Stop()
}
