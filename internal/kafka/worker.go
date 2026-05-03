package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

func KafkaConsumer(k *kgo.Client, ctx context.Context) {
	for {
		fetches := k.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Print(errs)
			return
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			fmt.Println(string(record.Value), "from an iterator!")
		}
	}
}
