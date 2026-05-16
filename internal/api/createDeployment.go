package deployment

import (
	"Deductio/internal/outbox"
	db "Deductio/internal/platform/storage/sqlc"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
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

type DeploymentCreatedEvent struct {
	DeploymentID uuid.UUID
	Application  string
	Version      string
	Environment  string
}

type Handler struct {
	Queries *db.Queries
	DB      *pgxpool.Pool
	Dh      *DeploymentHandler
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
	tx, err := h.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return db.Deployment{}, nil
	}
	defer tx.Rollback(ctx)
	creationTime := time.Now()
	pgCreationTime := pgtype.Timestamp{
		Time:  creationTime,
		Valid: true,
	}

	deploymentData, err := h.Queries.WithTx(tx).InsertDeployment(ctx, db.InsertDeploymentParams{
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
		row, err := h.Queries.SelectDeploymentIdKey(ctx, idempotencyKey)
		if err != nil {
			log.Print("This deployment row already exist in the database")
		}
		return row, err
	}

	//outbox pattern

	event := DeploymentCreatedEvent{
		DeploymentID: deploymentData.DeploymentID,
		Application:  deploymentData.Application,
		Version:      deploymentData.Version,
		Environment:  deploymentData.Environment,
	}

	msg, err := outbox.NewMessage(deploymentData.DeploymentID, "deployment", "deployment.created", event)
	if err != nil {
		log.Printf("Error trying to create new outbox msg: %v", err)
		return db.Deployment{}, err
	}

	_, err = h.Queries.WithTx(tx).InsertOutboxRow(ctx, db.InsertOutboxRowParams{
		msg.AggregateType, msg.AggregateID, msg.EventType, msg.Payload, msg.CreatedAt,
	})
	if err == sql.ErrNoRows {
		if err != nil {
			log.Print("This deployment row already exist in the database")
		}
		return db.Deployment{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return db.Deployment{}, err
	}
	fmt.Printf("Deploying %s v:%s to %s\n", req.Application, req.Version, req.Environment)
	return deploymentData, nil
}

func (h *Handler) CreateDeployment(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	keyHeader := r.Header.Get("X-Idempotency-Key")
	if keyHeader == "" {
		http.Error(w, "X-Idempotency-Key header is required", http.StatusBadRequest)
		return
	}

	idempotencyKey, err := uuid.Parse(keyHeader)
	if err != nil {
		http.Error(w, "Invalid IdempotencyKey format", http.StatusBadRequest)
		return
	}

	if !h.Dh.Health.IsSystemReady() {
		http.Error(w, "Deployment tool is unavailable", http.StatusServiceUnavailable)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Only POST request allowed", http.StatusMethodNotAllowed)
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
	response, err := h.CreateRequest(ctx, req, idempotencyKey)
	if err != nil {
		http.Error(w, "Failed to create deployment", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Idempotency-Key", keyHeader)
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(response)
}
