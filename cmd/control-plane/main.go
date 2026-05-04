package main

import (
	deployment "Deductio/internal/api"
	db "Deductio/internal/platform/storage/sqlc"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.DefaultProduceTopic("deployments"),
		kgo.ClientID("control-plane"),
	)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	dsn := getEnv("DSN", "")
	dbPool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer dbPool.Close()

	health := deployment.NewHealthHandler()
	dh := deployment.NewDeploymentHandler(deployment.DeploymentService{}, deployment.Logger{}, *health)
	h := &deployment.Handler{
		Queries: db.New(dbPool),
		DB:      dbPool,
		Dh:      dh,
		Kafka:   &deployment.Kafka{Client: client},
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Deductio"))
	})

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "Ok\n")
	})

	http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "Ok\n")
	})

	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Prometheus"))
	})

	http.HandleFunc("/deployments", func(w http.ResponseWriter, r *http.Request) {
		_, err := h.CreateDeployment(w, r)
		if err != nil {
			log.Printf("Deployment failed: %v", err)
		}
	})

	log.Fatal(http.ListenAndServe(":8081", nil))
}

func getEnv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}
