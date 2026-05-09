package outbox

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

type Message struct {
	ID            int64              `db:"id"`
	AggregateType string             `db:"aggregate_type"`
	AggregateID   uuid.UUID          `db:"aggregate_id"`
	EventType     string             `db:"event_type"`
	Payload       json.RawMessage    `db:"payload"`
	CreatedAt     pgtype.Timestamptz `db:"created_at"`
	ProcessedAt   *time.Time         `db:"processed_at"`
}

func NewMessage(aggregateID uuid.UUID, aggregateType, eventType string, payload any) (*Message, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	timeNow := pgtype.Timestamptz{
		Time:  time.Now(),
		Valid: true,
	}
	return &Message{
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		EventType:     eventType,
		Payload:       data,
		CreatedAt:     timeNow,
	}, nil
}
