-- name: InsertDeployment :one
INSERT INTO deployments (application, version, environment, current_status, last_error_status, created_at, updated_at, idempotency_key) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) 
ON CONFLICT (idempotency_key) DO NOTHING RETURNING *;

-- name: SelectDeployment :one
SELECT * FROM deployments WHERE idempotency_key = $1;
