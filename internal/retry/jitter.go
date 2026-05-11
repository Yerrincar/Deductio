package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
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
	return time.Duration(max * (b.RandGen.Float64() + 0.5))
}

type Retrier struct {
	MaxAttempts int
	Backoff     BackoffStrategy
}

func (r *Retrier) Do(ctx context.Context, operation func() error) error {
	var err error

	for attempt := 0; attempt < r.MaxAttempts; attempt++ {
		err = operation()
		if err == nil {
			return nil
		}

		wait := r.Backoff.NextBackoff(attempt)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("all %d retry attempts failed: %w", r.MaxAttempts, err)
}
