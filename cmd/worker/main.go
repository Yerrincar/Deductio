package main

import (
	"Deductio/internal/kafka"
	db "Deductio/internal/platform/storage/sqlc"
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx := context.Background()
	setupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ClientID("worker"),
		kgo.ConsumerGroup("workers"),
		kgo.ConsumeTopics("deployment-events"),
	)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	dsn := getEnv("DSN", "")
	dbPool, err := pgxpool.New(setupCtx, dsn)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer dbPool.Close()

	handler := &kafka.KafkaConsumerHandler{
		Client:  client,
		Queries: db.New(dbPool),
	}

	handler.KafkaConsumer(ctx)
}

func getEnv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}
