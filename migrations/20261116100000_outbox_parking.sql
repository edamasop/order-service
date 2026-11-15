-- +goose Up
-- +goose StatementBegin
-- A poison event used to be retried forever: every lease expiry made the
-- poller pick it up again, and an event that can never be published kept
-- waking the poller and producing warnings. "Parked" is a deliberate terminal
-- state that keeps the row for inspection instead of dropping it.
ALTER TABLE orders_outbox
    ADD COLUMN IF NOT EXISTS parked BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS last_error TEXT;

-- Claiming must skip parked rows; recreate the partial index accordingly.
DROP INDEX IF EXISTS idx_orders_outbox_claim;
CREATE INDEX IF NOT EXISTS idx_orders_outbox_claim
    ON orders_outbox (created_at, id)
    WHERE published = FALSE AND parked = FALSE;

-- Retention cleanup deletes old published rows by processed_at.
CREATE INDEX IF NOT EXISTS idx_orders_outbox_processed
    ON orders_outbox (processed_at)
    WHERE published = TRUE;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_orders_outbox_processed;
DROP INDEX IF EXISTS idx_orders_outbox_claim;
CREATE INDEX IF NOT EXISTS idx_orders_outbox_claim
    ON orders_outbox (created_at, id)
    WHERE published = FALSE;

ALTER TABLE orders_outbox
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS parked;
-- +goose StatementEnd
