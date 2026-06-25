package postgres

import (
	"context"
	"order-service/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRepository struct {
	db *pgxpool.Pool
}

func NewOutboxRepository(db *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{
		db: db,
	}
}

func (r *OutboxRepository) querier(ctx context.Context) Querier {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}

	return r.db
}

func (r *OutboxRepository) Create(ctx context.Context, event *model.OutboxEvent) error {
	q := r.querier(ctx)

	return q.QueryRow(
		ctx,
		`
		INSERT INTO orders_outbox (
			aggregate_id,
			aggregate_type,
			event_type,
			payload,
			published
		)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING
			id,
			created_at,
			processed_at
		`,
		event.AggregateID,
		event.EventType,
		event.Payload,
		event.Published,
	).Scan(
		&event.ID,
		&event.CreatedAt,
		&event.ProcessedAt,
	)
}

func (r *OutboxRepository) GetByID(ctx context.Context, id int64) (*model.OutboxEvent, error) {
	q := r.querier(ctx)

	event := new(model.OutboxEvent)

	err := q.QueryRow(
		ctx,
		`
		SELECT
			id,
			aggregate_id,
			aggregate_type,
			event_type,
			payload,
			created_at,
			processed_at,
			published
		FROM orders_outbox
		WHERE id = $1
		`,
		id,
	).Scan(
		&event.ID,
		&event.AggregateID,
		&event.EventType,
		&event.Payload,
		&event.CreatedAt,
		&event.ProcessedAt,
		&event.Published,
	)

	if err != nil {
		return nil, err
	}

	return event, nil
}

func (r *OutboxRepository) Update(ctx context.Context, event *model.OutboxEvent) error {
	q := r.querier(ctx)

	_, err := q.Exec(
		ctx,
		`
		UPDATE orders_outbox
		SET
			aggregate_id = $2,
			aggregate_type = $3,
			event_type = $4,
			payload = $5,
			processed_at = $6,
			published = $7
		WHERE id = $1
		`,
		event.ID,
		event.AggregateID,
		event.EventType,
		event.Payload,
		event.ProcessedAt,
		event.Published,
	)

	return err
}

func (r *OutboxRepository) Delete(ctx context.Context, id int64) error {
	q := r.querier(ctx)

	_, err := q.Exec(
		ctx,
		`DELETE FROM orders_outbox WHERE id = $1`,
		id,
	)

	return err
}

func (r *OutboxRepository) GetUnpublished(ctx context.Context, limit int) ([]*model.OutboxEvent, error) {
	q := r.querier(ctx)

	rows, err := q.Query(ctx, `
		SELECT
			id,
			aggregate_id,
			aggregate_type,
			event_type,
			payload,
			created_at,
			processed_at,
			published
		FROM orders_outbox
		WHERE published = FALSE
		ORDER BY created_at
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]*model.OutboxEvent, 0, limit)

	for rows.Next() {
		event := new(model.OutboxEvent)

		err = rows.Scan(
			&event.ID,
			&event.AggregateID,
			&event.EventType,
			&event.Payload,
			&event.CreatedAt,
			&event.ProcessedAt,
			&event.Published,
		)
		if err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, id int64) error {
	q := r.querier(ctx)

	_, err := q.Exec(ctx, `
		UPDATE orders_outbox
		SET
			published = TRUE,
			processed_at = now()
		WHERE id = $1
	`, id)

	return err
}
