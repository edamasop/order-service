# Load environment variables from .env file
include .env
export

# Directory where your migration files are stored
MIGRATIONS_DIR=./migrations
# Database driver
DB_DRIVER=postgres
# Schema this service owns (see POSTGRES_DSN / search_path in .env).
# The name contains a hyphen and is therefore always quoted in SQL.
SCHEMA=order-service
# The shared infrastructure compose file lives one level up, in the workspace root
COMPOSE_FILE=../docker-compose.yaml

# Create a new migration file. Usage: make migrate-new NAME=create_users_table
migrate-new:
	goose -dir $(MIGRATIONS_DIR) create $(NAME) sql

# Create the service schema. goose stores its version table inside search_path,
# so it cannot create the schema itself and fails with
# "no schema has been selected to create in" on a fresh database.
schema-up:
	docker compose -f $(COMPOSE_FILE) exec -T postgres \
		psql -U myuser -d mydb -v ON_ERROR_STOP=1 \
		-c 'CREATE SCHEMA IF NOT EXISTS "$(SCHEMA)"'

# Apply all available migrations
migrate-up: schema-up
	goose -dir $(MIGRATIONS_DIR) $(DB_DRIVER) "$(POSTGRES_DSN)" up

# Roll back the last migration
migrate-down:
	goose -dir $(MIGRATIONS_DIR) $(DB_DRIVER) "$(POSTGRES_DSN)" down

# Check the status of migrations
migrate-status:
	goose -dir $(MIGRATIONS_DIR) $(DB_DRIVER) "$(POSTGRES_DSN)" status

# Roll back all migrations
migrate-reset:
	goose -dir $(MIGRATIONS_DIR) $(DB_DRIVER) "$(POSTGRES_DSN)" reset


.PHONY: migrate-new migrate-up migrate-down migrate-status migrate-reset schema-up run build