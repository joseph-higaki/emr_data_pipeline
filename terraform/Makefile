.PHONY: init plan apply destroy clean

# Generate tfvars file from .env
generate-tfvars:
	@echo "Generating terraform.tfvars from Airflow .env file..."
	@./generate_tfvars.sh

# Initialize Terraform
init:
	@terraform init

# Plan with auto-generated tfvars
plan: generate-tfvars
	@terraform plan

# Apply with auto-generated tfvars
apply: generate-tfvars
	@terraform apply

# Destroy with auto-generated tfvars
destroy: generate-tfvars
	@terraform destroy

# Clean up generated files
clean:
	@rm -f terraform.tfvars
	@echo "Cleaned up generated files"
