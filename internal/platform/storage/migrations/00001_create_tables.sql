-- +goose Up
CREATE TABLE "deployments" (
  "deployment_id" SERIAL PRIMARY KEY,
  "application" VARCHAR(255),
  "version" VARCHAR(50),
  "environment" VARCHAR(100),
  "current_status" VARCHAR(50),
  "last_error_status" VARCHAR(255),
  "created_at" TIMESTAMP,
  "updated_at" TIMESTAMP
);
CREATE TABLE "deployments_audit" (
  "event_id" SERIAL PRIMARY KEY,
  "deployment_id" INTEGER NOT NULL,
  "event_type" VARCHAR(100),
  "message" VARCHAR(255),
  "user" VARCHAR(100),
  "created_at" TIMESTAMP,
  CONSTRAINT "deployments_audit_deployment_fk"
    FOREIGN KEY ("deployment_id")
    REFERENCES "deployments" ("deployment_id")
    DEFERRABLE INITIALLY IMMEDIATE
);
-- +goose Down
DROP TABLE deployments_audit;
DROP TABLE deployments;
