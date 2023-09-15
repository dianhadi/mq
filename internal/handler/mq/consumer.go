package mq

import (
	"context"
	"log"

	"github.com/dianhadi/mq/pkg/mq"
)

func (h Handler) Consume(ctx context.Context, msg mq.Message) error {
	// do what you need to do here
	log.Println(msg.Time(), string(msg.Body()))
	msg.Done()
	// fmt.Println(err)
	// msg.Requeue()
	// time.Sleep(10 * time.Second)
	return nil
}
