SHELL=/bin/bash
DATETIME:=$(shell date -u +%Y%m%dT%H%M%SZ)
MINIO_COMPOSE_FILE=tests/minio/docker-compose.yaml

### This is the Terraform-generated header for dspace-submission-composer-dev. If ###
### this is a Lambda repo, uncomment the FUNCTION line below ###
### and review the other commented lines in the document. ###
ECR_NAME_DEV := dspace-submission-composer-dev
ECR_URL_DEV := 222053980223.dkr.ecr.us-east-1.amazonaws.com/dspace-submission-composer-dev
CPU_ARCH ?= $(shell cat .aws-architecture 2>/dev/null || echo \"linux/amd64\")
# FUNCTION_DEV := 
### End of Terraform-generated header ###

help: # Preview Makefile commands
	@awk 'BEGIN { FS = ":.*#"; print "Usage:  make <target>\n\nTargets:" } \
/^[-_[:alpha:]]+:.?*#/ { printf "  %-15s%s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ensure OS binaries aren't called if naming conflict with Make recipes
.PHONY: help venv install update test coveralls lint black mypy ruff safety lint-apply black-apply ruff-apply

##############################################
# Python environment and dependency commands
##############################################

install: .venv .git/hooks/pre-commit # Install Python dependencies and create virtual environment if not exists
	uv sync --dev

.venv: # Creates virtual environment if not found
	@echo "Creating virtual environment at .venv..."
	uv venv .venv

.git/hooks/pre-commit: # Sets up pre-commit hook if not setup
	@echo "Installing pre-commit hooks..."
	uv run pre-commit install

venv: .venv # Create the Python virtual environment

update: # Update Python dependencies
	uv lock --upgrade
	uv sync --dev


######################
# Unit test commands
######################

test: # Run tests and print a coverage report
	uv run coverage run --source=dsc -m pytest -vv
	uv run coverage report -m

coveralls: test # Write coverage data to an LCOV report
	uv run coverage lcov -o ./coverage/lcov.info

####################################
# Code quality and safety commands
####################################

lint: black mypy ruff safety # Run linters

black: # Run 'black' linter and print a preview of suggested changes
	uv run black --check --diff .

mypy: # Run 'mypy' linter
	uv run mypy .

ruff: # Run 'ruff' linter and print a preview of errors
	uv run ruff check .

safety: # Check for security vulnerabilities and verify Pipfile.lock is up-to-date
	uv run pip-audit

lint-apply: black-apply ruff-apply # Apply changes with 'black' and resolve 'fixable errors' with 'ruff'

black-apply: # Apply changes with 'black'
	uv run black .

ruff-apply: # Resolve 'fixable errors' with 'ruff'
	uv run ruff check --fix .

####################################
# MinIO local S3 commands
####################################

start-minio-server:
	docker compose --env-file .env -f $(MINIO_COMPOSE_FILE) up -d

stop-minio-server:
	docker compose --env-file .env -f $(MINIO_COMPOSE_FILE) stop

### Terraform-generated Developer Deploy Commands for Dev environment ###
check-arch:
	@ARCH_FILE=\".aws-architecture\"; \
	if [[ \"$(CPU_ARCH)\" != \"linux/amd64\" && \"$(CPU_ARCH)\" != \"linux/arm64\" ]]; then \
	 echo \"Invalid CPU_ARCH: $(CPU_ARCH)\"; exit 1; \
	 fi; \
	 	if [[ -f $$ARCH_FILE ]]; then \
				echo \"latest-$(shell echo $(CPU_ARCH) | cut -d'/' -f2)\" > .arch_tag; \
					else \\\n\t\techo \"latest\" > .arch_tag; \
					fi\n\ndist-dev: check-arch ## Build docker container (intended for developer-based manual build)\n\t@ARCH_TAG=$$(cat .arch_tag); \\\n\tdocker buildx inspect $(ECR_NAME_DEV) >/dev/null 2>&1 || docker buildx create --name $(ECR_NAME_DEV) --use; \\\n\tdocker buildx use $(ECR_NAME_DEV); \\\n\tdocker buildx build --platform $(CPU_ARCH) \\\n\t\t--load \\\n\t --tag $(ECR_URL_DEV):$$ARCH_TAG \\\n\t --tag $(ECR_URL_DEV):make-$$ARCH_TAG \\\n\t\t--tag $(ECR_URL_DEV):make-$(shell git describe --always) \\\n\t\t--tag $(ECR_NAME_DEV):$$ARCH_TAG \\\n\t\t.\n\npublish-dev: dist-dev ## Build, tag and push (intended for developer-based manual publish)\n\t@ARCH_TAG=$$(cat .arch_tag); \\\n\taws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $(ECR_URL_DEV); \\\n\tdocker push $(ECR_URL_DEV):$$ARCH_TAG; \\\n\tdocker push $(ECR_URL_DEV):make-$$ARCH_TAG; \\\n\tdocker push $(ECR_URL_DEV):make-$(shell git describe --always); \\\n echo \"Cleaning up dangling Docker images...\"; \\\n docker image prune -f --filter \"dangling=true\"\n\n### If this is a Lambda repo, uncomment the two lines below ###\n# update-lambda-dev: ## Updates the lambda with whatever is the most recent image in the ecr (intended for developer-based manual update)\n#\t@ARCH_TAG=$$(cat .arch_tag); \\\n#\taws lambda update-function-code \\\n#\t\t--region us-east-1 \\\n#\t\t--function-name $(FUNCTION_DEV) \\\n#\t\t--image-uri $(ECR_URL_DEV):make-$$ARCH_TAG\n\ndocker-clean: ## Clean up Docker detritus\n\t@ARCH_TAG=$$(cat .arch_tag); \\\n\techo \"Cleaning up Docker leftovers (containers, images, builders)\"; \\\n\tdocker rmi -f $(ECR_URL_DEV):$$ARCH_TAG; \\\n\tdocker rmi -f $(ECR_URL_DEV):make-$$ARCH_TAG; \\\n\tdocker rmi -f $(ECR_URL_DEV):make-$(shell git describe --always) || true; \\\n docker rmi -f $(ECR_NAME_DEV):$$ARCH_TAG || true; \\\n\tdocker buildx rm $(ECR_NAME_DEV) || true\n\t@rm -rf .arch_tag\n



check-arch:
	@ARCH_FILE=".aws-architecture"; \
	if [[ "$(CPU_ARCH)" != "linux/amd64" && "$(CPU_ARCH)" != "linux/arm64" ]]; then \
		echo "Invalid CPU_ARCH: $(CPU_ARCH)"; exit 1; \
	fi; \
    if [[ -f $$ARCH_FILE ]]; then \
  		echo "latest-$(shell echo $(CPU_ARCH) | cut -d'/' -f2)" > .arch_tag; \
	else \
		echo "latest" > .arch_tag; \
	fi
        
dist-dev: check-arch ## Build docker container (intended for developer-based manual build)
    @ARCH_TAG=$$(cat .arch_tag); \
    docker buildx inspect $(ECR_NAME_DEV) >/dev/null 2>&1 || docker buildx create --name $(ECR_NAME_DEV) --use; \
    docker buildx use $(ECR_NAME_DEV); \
    docker buildx build --platform $(CPU_ARCH) \
    		--load \
    		--tag $(ECR_URL_DEV):$$ARCH_TAG \
    		--tag $(ECR_URL_DEV):make-$$ARCH_TAG \
    		--tag $(ECR_URL_DEV):make-$(shell git describe --always) \
    		--tag $(ECR_NAME_DEV):$$ARCH_TAG \
    		.

publish-dev: dist-dev ## Build, tag and push (intended for developer-based manual publish)
	@ARCH_TAG=$$(cat .arch_tag); \
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $(ECR_URL_DEV); \
	docker push $(ECR_URL_DEV):$$ARCH_TAG; \
	docker push $(ECR_URL_DEV):make-$$ARCH_TAG; \
	docker push $(ECR_URL_DEV):make-$(shell git describe --always); \
	echo "Cleaning up dangling Docker images..."; \
	docker image prune -f --filter "dangling=true"
        
    ### If this is a Lambda repo, uncomment the two lines below     ###
    # update-lambda-dev: ## Updates the lambda with whatever is the most recent image in the ecr (intended for developer-based manual update)
    #	@ARCH_TAG=$$(cat .arch_tag); \
    #	aws lambda update-function-code \
    #		--region us-east-1 \
    #		--function-name $(FUNCTION_DEV) \
    #		--image-uri $(ECR_URL_DEV):make-$$ARCH_TAG

	docker-clean: ## Clean up Docker detritus
    	@ARCH_TAG=$$(cat .arch_tag); \
    	echo "Cleaning up Docker leftovers (containers, images, builders)"; \
    	docker rmi -f $(ECR_URL_DEV):$$ARCH_TAG; \
    	docker rmi -f $(ECR_URL_DEV):make-$$ARCH_TAG; \
    	docker rmi -f $(ECR_URL_DEV):make-$(shell git describe --always) || true; \
    	docker rmi -f $(ECR_NAME_DEV):$$ARCH_TAG || true; \
    	docker buildx rm $(ECR_NAME_DEV) || true
    	@rm -rf .arch_tag
