-- +goose Up 
--constraint down first
ALTER TABLE "deployments_audit" 
    DROP CONSTRAINT "deployments_audit_deployment_fk";

--new data type for deployment_id
ALTER TABLE "deployments" ADD COLUMN "idempotency_key" UUID UNIQUE NOT NULL;

ALTER TABLE "deployments" ALTER COLUMN "deployment_id" DROP DEFAULT;
ALTER TABLE "deployments" ALTER COLUMN "deployment_id" TYPE UUID USING (gen_random_uuid());
ALTER TABLE "deployments" ALTER COLUMN "deployment_id" SET DEFAULT gen_random_uuid();

--new data type for deployment_id in secondary table
ALTER TABLE "deployments_audit" 
    ALTER COLUMN "deployment_id" TYPE UUID USING (NULL);

--add constraint again
ALTER TABLE "deployments_audit" 
    ADD CONSTRAINT "deployments_audit_deployment_fk" 
    FOREIGN KEY ("deployment_id") 
    REFERENCES "deployments" ("deployment_id")
    DEFERRABLE INITIALLY IMMEDIATE;
-- +goose Down
--rollback to v1 in case we need it
ALTER TABLE "deployments_audit" 
    DROP CONSTRAINT "deployments_audit_deployment_fk";

ALTER TABLE "deployments" DROP COLUMN "idempotency_key";

ALTER TABLE "deployments" ALTER COLUMN "deployment_id" DROP DEFAULT;

ALTER TABLE "deployments" 
    ALTER COLUMN "deployment_id" TYPE INTEGER USING (NULL);

CREATE SEQUENCE IF NOT EXISTS deployments_deployment_id_seq;
ALTER TABLE "deployments" 
    ALTER COLUMN "deployment_id" SET DEFAULT nextval('deployments_deployment_id_seq');
ALTER SEQUENCE deployments_deployment_id_seq OWNED BY "deployments"."deployment_id";

ALTER TABLE "deployments_audit" 
    ALTER COLUMN "deployment_id" TYPE INTEGER USING (NULL);

ALTER TABLE "deployments_audit" 
    ADD CONSTRAINT "deployments_audit_deployment_fk" 
    FOREIGN KEY ("deployment_id") 
    REFERENCES "deployments" ("deployment_id")
    DEFERRABLE INITIALLY IMMEDIATE;
