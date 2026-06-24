-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_customer_id
    ON orders(customer_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_status
    ON orders(status);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_outbox_published
    ON orders_outbox(published);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_outbox_created_at
    ON orders_outbox(created_at);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_orders_outbox_created_at;
DROP INDEX CONCURRENTLY IF EXISTS idx_orders_outbox_published;
DROP INDEX CONCURRENTLY IF EXISTS idx_orders_status;
DROP INDEX CONCURRENTLY IF EXISTS idx_orders_customer_id;
