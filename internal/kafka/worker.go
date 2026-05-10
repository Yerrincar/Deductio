package kafka

import (
	"Deductio/internal/gitops"
	db "Deductio/internal/platform/storage/sqlc"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type DeployemntEvent struct {
	DeploymentID uuid.UUID `json:"deployment_id"`
	Application  string    `json:"application"`
	Version      string    `json:"version"`
	Environment  string    `json:"environment"`
}

type KafkaConsumerHandler struct {
	Client  *kgo.Client
	Queries *db.Queries
}

func (k *KafkaConsumerHandler) KafkaConsumer(ctx context.Context) {
	for {
		fetches := k.Client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Print(errs)
			return
		}
		var event DeployemntEvent
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			fmt.Println(string(record.Value), "from an iterator!")
			err := json.Unmarshal(record.Value, &event)
			if err != nil {
				log.Printf("Error trying to unmarshal kafka message data: %v", err)
			}
			_, err = k.Queries.UpdateAsProcessing(ctx, event.DeploymentID)
			if err == sql.ErrNoRows {
				row, err := k.Queries.SelectDeploymentDepKey(ctx, event.DeploymentID)
				if err != nil {
					log.Printf("This deployment row is not in the requested state. Row: %v, \nError: %v", row, err)
				}
			}
			//simulate work for now
			time.Sleep(time.Second * 2)
			_, err = k.Queries.UpdateAsCompleted(ctx, event.DeploymentID)
			if err == sql.ErrNoRows {
				row, err := k.Queries.SelectDeploymentDepKey(ctx, event.DeploymentID)
				if err != nil {
					log.Printf("This deployment row is not in the processing state. Row: %v, \nError: %v", row, err)
				}
			}
		}
	}
}

func (k *KafkaConsumerHandler) WorkerLogic(ctx context.Context, deploymentId uuid.UUID) error {
	deployment, err := k.Queries.SelectDeploymentConsumerInfo(ctx, deploymentId)
	if err == sql.ErrNoRows {
		log.Printf("Row with deploymentId: %v doesn't exist in the DB. Error: %v", deploymentId, err)
	}

	if deployment.Application == "" || deployment.Version == "" || deployment.Environment == "" {
		return errors.New("Deployment data is empty")
	}

	if err := gitops.UpdateApplicationVersion(gitops.ValuesFilePath, deployment.Application, deployment.Version); err != nil {
		return err
	}

	return nil
}
