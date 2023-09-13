package http

import "github.com/dianhadi/mq/pkg/mq"

type Handler struct {
	producer mq.Producer
}

func New(producer mq.Producer) (*Handler, error) {
	return &Handler{
		producer: producer,
	}, nil
}
