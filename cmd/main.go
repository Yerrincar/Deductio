package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {

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

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func getEnv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}
