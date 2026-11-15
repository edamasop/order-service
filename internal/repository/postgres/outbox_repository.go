package postgres

import (
	"context"
	"time"

	"order-service/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// outboxColumns is the single source of truth for column order. It must
// match the order in which outboxFields() writes into model.OutboxEvent.
const outboxColumns = `id, aggregate_id, event_type, payload, created_at,
	processed_at, published, locked_until, attempts, parked, last_error`

// outboxColumnsAliased is the same column list, prefixed for contexts where
// the table has an alias (e.g. the "outbox" alias in ClaimUnpublished's
// UPDATE ... FROM).
const outboxColumnsAliased = `outbox.id, outbox.aggregate_id, outbox.event_type,
	outbox.payload, outbox.created_at, outbox.processed_at, outbox.published,
	outbox.locked_until, outbox.attempts, outbox.parked, outbox.last_error`

const outboxSelect = `SELECT ` + outboxColumns + ` FROM orders_outbox`

type OutboxRepository struct {
	db *pgxpool.Pool
}

func NewOutboxRepository(db *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{db: db}
}

func (r *OutboxRepository) querier(ctx context.Context) Querier {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return r.db
}

func (r *OutboxRepository) Create(ctx context.Context, event *model.OutboxEvent) error {
	return r.querier(ctx).QueryRow(ctx, `
		INSERT INTO orders_outbox (aggregate_id, event_type, payload)
		VALUES ($1, $2, $3)
		RETURNING id, created_at, processed_at, published, locked_until, attempts, parked, last_error`,
		event.AggregateID, event.EventType, event.Payload,
	).Scan(
		&event.ID,
		&event.CreatedAt,
		&event.ProcessedAt,
		&event.Published,
		&event.LockedUntil,
		&event.Attempts,
		&event.Parked,
		&event.LastError,
	)
}

func (r *OutboxRepository) GetByID(ctx context.Context, id int64) (*model.OutboxEvent, error) {
	event := new(model.OutboxEvent)
	err := r.querier(ctx).QueryRow(ctx, outboxSelect+` WHERE id = $1`, id).Scan(outboxFields(event)...)
	if err != nil {
		return nil, err
	}
	return event, nil
}

func (r *OutboxRepository) Update(ctx context.Context, event *model.OutboxEvent) error {
	_, err := r.querier(ctx).Exec(ctx, `
		UPDATE orders_outbox
		SET aggregate_id = $2, event_type = $3, payload = $4,
			processed_at = $5, published = $6, locked_until = $7
		WHERE id = $1`,
		event.ID, event.AggregateID, event.EventType, event.Payload,
		event.ProcessedAt, event.Published, event.LockedUntil,
	)
	return err
}

func (r *OutboxRepository) Delete(ctx context.Context, id int64) error {
	_, err := r.querier(ctx).Exec(ctx, `DELETE FROM orders_outbox WHERE id = $1`, id)
	return err
}

func (r *OutboxRepository) GetUnpublished(ctx context.Context, limit int) ([]*model.OutboxEvent, error) {
	rows, err := r.querier(ctx).Query(ctx, outboxSelect+`
		WHERE published = FALSE
		ORDER BY created_at, id
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return scanOutboxEvents(rows)
}

// ClaimUnpublished leases events using SKIP LOCKED, so concurrent pollers do
// not publish the same event during the lease period. Delivery remains
// at-least-once; consumers must be idempotent.
func (r *OutboxRepository) ClaimUnpublished(ctx context.Context, limit int, lease time.Duration) ([]*model.OutboxEvent, error) {
	rows, err := r.db.Query(ctx, `
		WITH candidates AS (
			SELECT id
			FROM orders_outbox
			WHERE published = FALSE
				AND parked = FALSE
				AND (locked_until IS NULL OR locked_until < now())
			ORDER BY created_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE orders_outbox AS outbox
		SET locked_until = now() + ($2 * interval '1 millisecond'),
			attempts = attempts + 1
		FROM candidates
		WHERE outbox.id = candidates.id
		RETURNING `+outboxColumnsAliased, limit, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	return scanOutboxEvents(rows)
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, id int64) error {
	_, err := r.querier(ctx).Exec(ctx, `
		UPDATE orders_outbox
		SET published = TRUE, processed_at = now(), locked_until = NULL
		WHERE id = $1`, id)
	return err
}

// Park moves an event that exhausted its retries into the parking lot. The row
// is kept (with the last error) so it can be inspected or replayed by hand; it
// is no longer claimed by the poller.
func (r *OutboxRepository) Park(ctx context.Context, id int64, reason string) error {
	_, err := r.querier(ctx).Exec(ctx, `
		UPDATE orders_outbox
		SET parked = TRUE, locked_until = NULL, last_error = $2
		WHERE id = $1`, id, reason)
	return err
}

// DeletePublishedBefore is the retention cleanup: published rows older than
// the cutoff are removed so the table does not grow forever.
func (r *OutboxRepository) DeletePublishedBefore(ctx context.Context, before time.Time) (int64, error) {
	tag, err := r.querier(ctx).Exec(ctx, `
		DELETE FROM orders_outbox
		WHERE published = TRUE AND processed_at < $1`, before)
	if err != nil {
		return 0, err
	}

	return tag.RowsAffected(), nil
}

// CountPending reports the outbox backlog: rows waiting to be published and
// rows parked after exhausting their retries.
func (r *OutboxRepository) CountPending(ctx context.Context) (pending int64, parked int64, err error) {
	err = r.querier(ctx).QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE published = FALSE AND parked = FALSE),
			count(*) FILTER (WHERE parked = TRUE)
		FROM orders_outbox`).Scan(&pending, &parked)

	return pending, parked, err
}

func outboxFields(event *model.OutboxEvent) []any {
	return []any{
		&event.ID, &event.AggregateID, &event.EventType, &event.Payload,
		&event.CreatedAt, &event.ProcessedAt, &event.Published, &event.LockedUntil,
		&event.Attempts, &event.Parked, &event.LastError,
	}
}

func scanOutboxEvents(rows pgx.Rows) ([]*model.OutboxEvent, error) {
	defer rows.Close()

	events := make([]*model.OutboxEvent, 0)
	for rows.Next() {
		event := new(model.OutboxEvent)
		if err := rows.Scan(outboxFields(event)...); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}
