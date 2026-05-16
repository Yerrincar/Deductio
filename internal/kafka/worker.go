package kafka

import (
	"Deductio/internal/gitops"
	db "Deductio/internal/platform/storage/sqlc"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			var event DeployemntEvent
			err := json.Unmarshal(record.Value, &event)
			if err != nil {
				log.Printf("Error trying to unmarshal kafka message data: %v", err)
				continue
			}
			_, err = k.Queries.UpdateAsProcessing(ctx, event.DeploymentID)
			if err == sql.ErrNoRows {
				row, err := k.Queries.SelectDeploymentDepKey(ctx, event.DeploymentID)
				if err == nil {
					log.Printf("Skipping deployment %s with current status %s", row.DeploymentID, row.CurrentStatus)
				}
				continue
			}
			if err != nil {
				log.Printf("Error trying to claim deployment %s: %v", event.DeploymentID, err)
				continue
			}

			err = k.WorkerLogic(ctx, event.DeploymentID)
			if err != nil {
				log.Printf("Worker logic failed for deployment %s: %v", event.DeploymentID, err)
				continue
			}

			// Simulate work for now while the worker updates GitOps state.
			time.Sleep(time.Second * 2)
			_, err = k.Queries.UpdateAsCompleted(ctx, event.DeploymentID)
			if err == sql.ErrNoRows {
				row, err := k.Queries.SelectDeploymentDepKey(ctx, event.DeploymentID)
				if err == nil {
					log.Printf("Deployment %s did not complete because current status is %s", row.DeploymentID, row.CurrentStatus)
				}
				continue
			}
			if err != nil {
				log.Printf("Error trying to complete deployment %s: %v", event.DeploymentID, err)
			}
		}
	}
}

func (k *KafkaConsumerHandler) WorkerLogic(ctx context.Context, deploymentID uuid.UUID) error {
	deployment, err := k.Queries.SelectDeploymentConsumerInfo(ctx, deploymentID)
	if err == sql.ErrNoRows {
		log.Printf("Row with deploymentId: %v doesn't exist in the DB. Error: %v", deploymentID, err)
		return err
	}
	if err != nil {
		return err
	}

	if deployment.Application == "" || deployment.Version == "" || deployment.Environment == "" {
		return errors.New("Deployment data is empty")
	}

	if err := gitops.UpdateApplicationVersion(gitops.ValuesFilePath, deployment.Application, deployment.Version); err != nil {
		return err
	}

	return nil
}
