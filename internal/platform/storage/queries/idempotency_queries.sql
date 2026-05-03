-- name: IdempotencyCleanup :exec 
DELETE FROM Idempotency_keys WHERE created_at < NOW() - INTERVAL '24 hours';
