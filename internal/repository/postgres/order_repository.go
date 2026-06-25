package postgres

import (
	"context"
	"order-service/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OrderRepository struct {
	db *pgxpool.Pool
}

func NewOrderRepository(db *pgxpool.Pool) *OrderRepository {
	return &OrderRepository{
		db: db,
	}
}

func (r *OrderRepository) querier(ctx context.Context) Querier {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}

	return r.db
}

func (r *OrderRepository) Create(ctx context.Context, order *model.Order) error {
	q := r.querier(ctx)

	return q.QueryRow(ctx, `
		INSERT INTO orders
		(order_number, customer_id, status, total_amount, currency)
		VALUES ($1,$2,$3,$4,$5)
		RETURNING id, created_at, updated_at`,
		order.OrderNumber,
		order.CustomerID,
		order.Status,
		order.TotalAmount,
		order.Currency,
	).Scan(
		&order.ID,
		&order.CreatedAt,
		&order.UpdatedAt,
	)
}

func (r *OrderRepository) GetByID(ctx context.Context, id int64) (*model.Order, error) {
	q := r.querier(ctx)

	order := new(model.Order)

	err := q.QueryRow(ctx, `
		SELECT
			id,
			order_number,
			customer_id,
			status,
			total_amount,
			currency,
			created_at,
			updated_at
		FROM orders
		WHERE id=$1`,
		id,
	).Scan(
		&order.ID,
		&order.OrderNumber,
		&order.CustomerID,
		&order.Status,
		&order.TotalAmount,
		&order.Currency,
		&order.CreatedAt,
		&order.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return order, nil
}

func (r *OrderRepository) Update(ctx context.Context, order *model.Order) error {
	q := r.querier(ctx)

	_, err := q.Exec(ctx, `
		UPDATE orders
		SET
			order_number=$2,
			customer_id=$3,
			status=$4,
			total_amount=$5,
			currency=$6,
			updated_at=now()
		WHERE id=$1`,
		order.ID,
		order.OrderNumber,
		order.CustomerID,
		order.Status,
		order.TotalAmount,
		order.Currency,
	)

	return err
}

func (r *OrderRepository) Delete(ctx context.Context, id int64) error {
	q := r.querier(ctx)

	_, err := q.Exec(ctx,
		`DELETE FROM orders WHERE id=$1`,
		id,
	)

	return err
}
