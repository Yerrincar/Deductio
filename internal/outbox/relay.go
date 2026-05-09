package outbox

import (
	db "Deductio/internal/platform/storage/sqlc"
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Publisher interface {
	Publish(ctx context.Context, topic string, key uuid.UUID, value []byte) error
}

type Relay struct {
	DB        *pgxpool.Pool
	Queries   *db.Queries
	Publisher Publisher
	BatchSize int
	Interval  time.Duration
}

func NewRelay(dbPool *pgxpool.Pool, publisher Publisher) *Relay {
	return &Relay{
		DB:        dbPool,
		Queries:   db.New(dbPool),
		Publisher: publisher,
		BatchSize: 100,
		Interval:  time.Second,
	}
}

func (r *Relay) Start(ctx context.Context) {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.processBatch(ctx); err != nil {
				log.Printf("outbox relay error: %v", err)
			}
		}
	}
}

func (r *Relay) processBatch(ctx context.Context) error {
	messages, err := r.Queries.SelectUnprocessedMsg(ctx, int32(r.BatchSize))
	if err != nil {
		return err
	}

	for _, msg := range messages {
		topic := msg.AggregateType + "-events"

		if err := r.Publisher.Publish(ctx, topic, msg.AggregateID, msg.Payload); err != nil {
			log.Printf("failed to publish message %d: %v", msg.DeploymentID, err)
			continue
		}

		_, err := r.Queries.UpdateAsProcessed(ctx, msg.DeploymentID)
		if err != nil {
			log.Printf("failed to mark message %d as processed: %v", msg.DeploymentID, err)
		}
	}

	return nil
}
