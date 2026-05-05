package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type BackoffStrategy interface {
	NextBackoff(attempt int) time.Duration
}

type ExponentialJitterBackoff struct {
	Base    time.Duration
	Cap     time.Duration
	RandGen *rand.Rand
}

func (b *ExponentialJitterBackoff) NextBackoff(attempt int) time.Duration {
	max := float64(b.Base) * math.Pow(2, float64(attempt))
	if max > float64(b.Cap) {
		max = float64(b.Cap)
	}
	jitter := b.RandGen.Float64() + 0.5
	return time.Duration(max * jitter)
}

type RetryPolicy interface {
	ShouldRetry(resp *http.Response, err error) bool
}

type StatusCodeRetryPolicy struct {
	RetryStatuses map[int]bool
}

func (p *StatusCodeRetryPolicy) ShouldRetry(resp *http.Response, err error) bool {
	if err != nil {
		return true
	}
	if resp != nil && p.RetryStatuses[resp.StatusCode] {
		return true
	}
	return false
}

type Retrier struct {
	MaxAttempts    int
	Backoff        BackoffStrategy
	RetryCondition RetryPolicy
}

func (r *Retrier) Do(ctx context.Context, operation func() (*http.Response, error)) (*http.Response, error) {
	var resp *http.Response
	var err error

	for attempt := 0; attempt < r.MaxAttempts; attempt++ {
		resp, err = operation()

		if !r.RetryCondition.ShouldRetry(resp, err) {
			return resp, err
		}

		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}

		wait := r.Backoff.NextBackoff(attempt)
		fmt.Printf("Retry attempt %d failed; waiting %v before next attempt. Error: %v\n", attempt+1, wait, err)

		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	fmt.Printf("All %d retry attempts failed. Final error: %v\n", r.MaxAttempts, err)
	return resp, fmt.Errorf("all %d retry attempts failed: %w", r.MaxAttempts, err)
}

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type RetryableHTTPClient struct {
	Client  HTTPClient
	Retrier *Retrier
}

func (c *RetryableHTTPClient) DoWithRetry(ctx context.Context, internalURL, idempotencyKey string, data []byte) (*http.Response, error) {
	return c.Retrier.Do(ctx, func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", internalURL, bytes.NewBuffer(data))
		if err != nil {
			log.Printf("Failed POST request to create deployment: %v", err)
			return nil, err
		}
		req.Header.Set("X-Idempotency-Key", idempotencyKey)
		req.Header.Set("Content-Type", "application/json")
		return c.Client.Do(req)
	})
}

func main() {

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	backoff := &ExponentialJitterBackoff{
		Base:    500 * time.Millisecond,
		Cap:     5 * time.Second,
		RandGen: r,
	}

	retryPolicy := &StatusCodeRetryPolicy{
		RetryStatuses: map[int]bool{
			500: true,
			502: true,
			503: true,
			504: true,
		},
	}

	retrier := &Retrier{
		MaxAttempts:    3,
		Backoff:        backoff,
		RetryCondition: retryPolicy,
	}

	client := &RetryableHTTPClient{
		Client:  &http.Client{Timeout: 10 * time.Second},
		Retrier: retrier,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	idempotencyKey := uuid.New().String()
	data := []byte(`{"application": "example", "version": "1", "environment": "testing"}`)
	internalURL := "http://localhost:8081/deployments"

	resp, err := client.DoWithRetry(ctx, internalURL, idempotencyKey, data)
	if err != nil {
		fmt.Println("Request failed:", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("Deployment request failed with status: %s", resp.Status)
		return
	}
}
