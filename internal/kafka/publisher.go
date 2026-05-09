package kafka

import (
	"context"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaPublisher struct {
	client *kgo.Client
}

func NewKafkaPublisher(client *kgo.Client) *KafkaPublisher {
	return &KafkaPublisher{client: client}
}

func (p *KafkaPublisher) Publish(ctx context.Context, topic string, key uuid.UUID, value []byte) error {
	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(key.String()),
		Value: value,
	}

	return p.client.ProduceSync(ctx, record).FirstErr()
}
