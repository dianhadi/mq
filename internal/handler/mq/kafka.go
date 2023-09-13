package mq

import (
	"context"
	"log"

	"github.com/dianhadi/mq/pkg/mq"
)

func (h Handler) Consume(ctx context.Context, msg mq.Message) error {
	// do what you need to do here
	log.Println(msg.Time(), string(msg.Body()))
	return nil
}
