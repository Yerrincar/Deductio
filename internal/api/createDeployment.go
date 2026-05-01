package deployment

import (
	db "Deductio/internal/platform/storage/sqlc"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
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

type Handler struct {
	Queries *db.Queries
	DB      *sql.DB
	dh      *DeploymentHandler
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
	return true
}

func (h *Handler) CreateRequest(ctx context.Context, req DeploymentRequest) error {
	//Future logic to create the Kafka Event and the insertion to db
	fmt.Printf("Deploying %s v:%s to %s\n", req.Application, req.Version, req.Environment)
	return nil
}

func (h *Handler) CreateDeployment(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

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

	deployemntData, err := h.Queries.InsertBooks(ctx, db.InsertBooksParams{
		Application: req.Application,
		Version:     req.Version,
		Environment: req.Environment,
	})
	//err := h.deploymentService.CreateRequest(req)
	if err != nil {
		http.Error(w, "Failed to create deployment", http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "deploymentStarted"})
}
