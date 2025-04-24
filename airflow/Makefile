.PHONY: up down clean

# PostgreSQL data directory path - updated for new location
PG_DATA_DIR = ./airflow_postgresql_data
# Env file path 
ENV_FILE = ./.env

check-env:
	@echo "Checking environment file..."
	@if [ ! -f $(ENV_FILE) ]; then \
		echo "Error: $(ENV_FILE) file not found"; \
		echo "Please create a .env file with the required variables"; \
		exit 1; \
	fi

# Load environment variables from .env file, 
# so that they are not taken from current session
# this is important if during the dev process, 
# you need to change the variables
env-load: check-env
	@echo "Loading environment variables from $(ENV_FILE)..."
	$(eval include $(ENV_FILE))
	$(eval export $(shell sed 's/=.*//' $(ENV_FILE)))

up: env-load
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
