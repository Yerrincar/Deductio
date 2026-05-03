package deployment

import (
	db "Deductio/internal/platform/storage/sqlc"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/twmb/franz-go/pkg/kgo"
)

type DeploymentRequest struct {
	Application string `json:"application"`
	Version     string `json:"version"`
	Environment string `json:"environment"`
}

type DeploymentService struct {
	deploymentID  int
	initialStatus string
}

type Logger struct {
	internalErrors error
	badRequests    string
}

type HealthService struct {
	internalUrl string
	HTTPClient  *http.Client
}

type DeploymentHandler struct {
	deploymentService DeploymentService
	health            HealthService
	logger            Logger
}

type Kafka struct {
	client *kgo.Client
}

type Handler struct {
	Queries *db.Queries
	DB      *sql.DB
	dh      *DeploymentHandler
	kafka   *Kafka
}

func NewDeploymentHandler(deployment DeploymentService, logger Logger, health HealthService) *DeploymentHandler {
	return &DeploymentHandler{
		deploymentService: deployment,
		logger:            logger,
		health:            health,
	}
}

func (h *HealthService) IsSystemReady() bool {
	resp, err := h.HTTPClient.Get(h.internalUrl + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	defer resp.Body.Close()
	return true
}

func (h *Handler) CreateRequest(ctx context.Context, req DeploymentRequest) (db.Deployment, error) {
	creationTime := time.Now()
	pgCreationTime := pgtype.Timestamp{
		Time:  creationTime,
		Valid: true,
	}

	deployementData, err := h.Queries.InsertDeployment(ctx, db.InsertDeploymentParams{
		Application:     req.Application,
		Version:         req.Version,
		Environment:     req.Environment,
		CurrentStatus:   "requested",
		LastErrorStatus: "no error",
		CreatedAt:       pgCreationTime,
		UpdatedAt:       pgCreationTime,
	})
	if err != nil {
		log.Print("Failed to insert deployment")
		return db.Deployment{}, err
	}
	payload, _ := json.Marshal(deployementData)
	//Idempotency in case publish fails needed
	err = h.KafkaProducer(ctx, req, payload)
	if err != nil {
		log.Print("Error trying to produce deployment.request event")
		return db.Deployment{}, err
	}
	fmt.Printf("Deploying %s v:%s to %s\n", req.Application, req.Version, req.Environment)
	return deployementData, nil
}

func (h *Handler) KafkaProducer(ctx context.Context, req DeploymentRequest, data []byte) error {
	//producers
	var wg sync.WaitGroup
	wg.Add(1)
	record := &kgo.Record{Topic: "deployments", Value: data}
	h.kafka.client.Produce(ctx, record, func(_ *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			log.Printf("record had a produce error: %v\n", err)
			return
		}
	})
	wg.Wait()

	return nil
}

func (h *Handler) CreateDeployment(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if !h.dh.health.IsSystemReady() {
		http.Error(w, "Deployment tool is unavailable", http.StatusServiceUnavailable)
		return
	}

	var req DeploymentRequest

	dec := json.NewDecoder(r.Body)

	if err := dec.Decode(&req); err != nil {
		http.Error(w, "Error decoding the request", http.StatusBadRequest)
		return
	}
	if req.Application == "" || req.Version == "" || req.Environment == "" {
		http.Error(w, "Error decoding the request", http.StatusBadRequest)
		return
	}

	response, err := h.CreateRequest(ctx, req)
	if err != nil {
		http.Error(w, "Failed to create deployment", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(response)
}
