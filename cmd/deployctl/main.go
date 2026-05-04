package main

import (
	"bytes"
	"log"
	"net/http"

	"github.com/google/uuid"
)

func main() {

	idempotencyKey := uuid.New().String()
	data := []byte(`{"application": "example", "version": "1", "environment": "testing"}`)

	req, err := http.NewRequest("POST", "http://localhost:8081/deployments", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("Failed POST request to create deployment: %v", err)
		return
	}

	req.Header.Set("X-Idempotency-Key", idempotencyKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Failed to send deployment request: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("Deployment request failed with status: %s", resp.Status)
		return
	}
}
