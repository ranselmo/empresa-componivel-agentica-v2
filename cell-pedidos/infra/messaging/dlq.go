package messaging

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type DLQMessage struct {
	OriginalTopic string          `json:"original_topic"`
	Key           string          `json:"key"`
	Payload       json.RawMessage `json:"payload"`
	Error         string          `json:"error"`
	Attempts      int             `json:"attempts"`
	FailedAt      time.Time       `json:"failed_at"`
}

func (pr *Producer) SendToDLQ(pbc, originalTopic, key string, payload []byte, err error, attempts int) {
	topic := fmt.Sprintf("dlq.%s.%s", pbc, originalTopic)
	msg := DLQMessage{OriginalTopic: originalTopic, Key: key,
		Payload: payload, Error: err.Error(),
		Attempts: attempts, FailedAt: time.Now().UTC()}
	b, _ := json.Marshal(msg)
	_ = pr.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key), Value: b,
	}, nil)
}
