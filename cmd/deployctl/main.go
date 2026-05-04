package main

import (
	deployment "Deductio/internal/api"
	db "Deductio/internal/platform/storage/sqlc"
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	opts := []kgo.Opt{
		kgo.SeedBrokers("localhost:9092"),
		kgo.DefaultProduceTopic("deployments"),
		kgo.ClientID("user"),
		kgo.ConsumerGroup("workers"),
		kgo.ConsumeTopics("deployments"),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Print("Failed to create Kafka client")
	}
	defer client.Close()

	dsn := getEnv("DSN", "")
	dbPool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer dbPool.Close()
	k := &deployment.Kafka{Client: client}
	h := &deployment.Handler{Queries: db.New(dbPool), DB: dbPool, Dh: &deployment.DeploymentHandler{}, Kafka: k}

	http.HandleFunc("/deployments", func(w http.ResponseWriter, r *http.Request) {
		idempotencyKey := uuid.New().String()
		r.Header.Set("X-Idempotency-Key", idempotencyKey)
		r.Header.Set("Content-Type", "application/json")
		_, err := h.CreateDeployment(w, r)
		if err != nil {
			log.Printf("Deployment Failed :%v", err)
		}
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func getEnv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}
