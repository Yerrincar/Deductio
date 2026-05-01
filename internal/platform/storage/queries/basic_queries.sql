-- name: InsertBooks :many
INSERT INTO deployments (application, version, environment, current_status, last_error_status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *;
