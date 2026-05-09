-- name: InsertDeployment :one
INSERT INTO deployments (application, version, environment, current_status, last_error_status, created_at, updated_at, idempotency_key) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) 
ON CONFLICT (idempotency_key) DO NOTHING RETURNING *;

-- name: SelectDeployment :one
SELECT * FROM deployments WHERE idempotency_key = $1;

-- name: InsertOutboxRow :one
INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING *;

-- name: SelectUnprocessedMsg :many
SELECT deployment_id, aggregate_type, aggregate_id, event_type, payload FROM outbox WHERE processed_at IS NULL ORDER BY created_at
LIMIT $1 FOR UPDATE SKIP LOCKED;

-- name: UpdateAsProcessed :one
UPDATE outbox SET processed_at = NOW() WHERE deployment_id = $1 RETURNING *;
