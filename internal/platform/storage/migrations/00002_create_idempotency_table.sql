-- +goose Up 
CREATE TABLE "idempotency_keys" (
  "key" UUID PRIMARY KEY,
  "status" VARCHAR(10) NOT NULL DEFAULT 'pending',
  "status_code" INT,
  "body" TEXT,
  "created_at" TIMESTAMPZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE deployments_audit;
