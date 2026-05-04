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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
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
	DeploymentService DeploymentService
	Health            HealthService
	Logger            Logger
}

type Kafka struct {
	Client *kgo.Client
}

type Handler struct {
	Queries *db.Queries
	DB      *pgxpool.Pool
	Dh      *DeploymentHandler
	Kafka   *Kafka
}

func NewHealthHandler() *HealthService {
	return &HealthService{
		internalUrl: "http://localhost:8081/healthz",
		HTTPClient:  &http.Client{},
	}
}

func NewDeploymentHandler(deployment DeploymentService, logger Logger, health HealthService) *DeploymentHandler {
	return &DeploymentHandler{
		DeploymentService: deployment,
		Logger:            logger,
		Health:            health,
	}
}

func (h *HealthService) IsSystemReady() bool {
	resp, err := h.HTTPClient.Get(h.internalUrl)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	defer resp.Body.Close()
	return true
}

func (h *Handler) CreateRequest(ctx context.Context, req DeploymentRequest, idempotencyKey uuid.UUID) (db.Deployment, error) {
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
		IdempotencyKey:  idempotencyKey,
	})
	if err == sql.ErrNoRows {
		row, err := h.Queries.SelectDeployment(ctx, idempotencyKey)
		if err != nil {
			log.Print("This deployment row already exist in the database")
		}
		return row, err
	}
	payload, _ := json.Marshal(deployementData)
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
	var errProducer error
	wg.Add(1)
	record := &kgo.Record{Topic: "deployments", Value: data}
	h.Kafka.Client.Produce(ctx, record, func(_ *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			log.Printf("record had a produce error: %v\n", err)
			errProducer = err
		}
	})
	wg.Wait()
	if errProducer != nil {
		return errProducer
	}
	return nil
}

func (h *Handler) CreateDeployment(w http.ResponseWriter, r *http.Request) (*DeploymentRequest, error) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	keyHeader := r.Header.Get("X-Idempotency-Key")
	if keyHeader == "" {
		http.Error(w, "X-Idempotency-Key header is required", http.StatusBadRequest)
		return nil, nil
	}

	idempotencyKey, err := uuid.Parse(keyHeader)
	if err != nil {
		http.Error(w, "Invalid IdempotencyKey format", http.StatusBadRequest)
		return nil, err
	}

	if !h.Dh.Health.IsSystemReady() {
		http.Error(w, "Deployment tool is unavailable", http.StatusServiceUnavailable)
		return nil, nil
	}

	if r.Method != "POST" {
		http.Error(w, "Only POST request allowed", http.StatusMethodNotAllowed)
		return nil, nil
	}

	var req DeploymentRequest

	dec := json.NewDecoder(r.Body)

	if err := dec.Decode(&req); err != nil {
		http.Error(w, "Error decoding the request", http.StatusBadRequest)
		return nil, err
	}
	if req.Application == "" || req.Version == "" || req.Environment == "" {
		http.Error(w, "Error decoding the request", http.StatusBadRequest)
		return nil, nil
	}
	response, err := h.CreateRequest(ctx, req, idempotencyKey)
	if err != nil {
		http.Error(w, "Failed to create deployment", http.StatusInternalServerError)
		return nil, err
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Idempotency-Key", keyHeader)
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(response)
	return &req, nil
}
