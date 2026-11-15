-- +goose Up
-- +goose StatementBegin
-- ClaimUnpublished() leases rows through these two columns, but the original
-- table definition never had them: every INSERT ... RETURNING and every
-- ClaimUnpublished call failed with "column locked_until does not exist".
ALTER TABLE orders_outbox
    ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;

-- Mirrors the ClaimUnpublished predicate (unpublished rows, insertion order).
CREATE INDEX IF NOT EXISTS idx_orders_outbox_claim
    ON orders_outbox (created_at, id)
    WHERE published = FALSE;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_orders_outbox_claim;

ALTER TABLE orders_outbox
    DROP COLUMN IF EXISTS attempts,
    DROP COLUMN IF EXISTS locked_until;
-- +goose StatementEnd
