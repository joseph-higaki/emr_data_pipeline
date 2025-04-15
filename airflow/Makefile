.PHONY: up down clean

# PostgreSQL data directory path - updated for new location
PG_DATA_DIR = ./airflow_postgresql_data

up:
	@echo "Starting Docker containers..."
	@# Check if PostgreSQL data directory exists and fix permissions if it does
	@if [ -d "$(PG_DATA_DIR)" ]; then \
		echo "Fixing PostgreSQL data directory permissions..."; \
		sudo chmod -R 770 $(PG_DATA_DIR); \
	fi
	@echo "Running docker-compose up..."
	@docker compose up -d

down:
	@echo "Stopping Docker containers..."
	@docker compose down
	@echo "Removing EMR images..."
	@docker rmi emr_ingestion:latest emr_transformation:latest 2>/dev/null || true
	@echo "Docker environment stopped and cleaned up."

# Add this target if you want to completely reset the environment
clean: down
	@echo "Removing PostgreSQL data directory..."
	@if [ -d "$(PG_DATA_DIR)" ]; then \
		sudo rm -rf $(PG_DATA_DIR); \
	fi
	@echo "Environment cleaned up."
