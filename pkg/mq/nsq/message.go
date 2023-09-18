package nsq

import (
	"time"

	"github.com/nsqio/go-nsq"
)

type NsqMessage struct {
	message *nsq.Message
}

func (m NsqMessage) Body() []byte {
	return m.message.Body
}

func (m NsqMessage) Time() time.Time {
	return time.Unix(0, m.message.Timestamp*int64(time.Millisecond))
}

func (m NsqMessage) Requeue() error {
	m.message.Requeue(0) // Requeue immediately
	return nil
}

func (m NsqMessage) Done() error {
	m.message.Finish()
	return nil
}
