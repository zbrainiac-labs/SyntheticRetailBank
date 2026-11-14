# Bank app - local Docker and SPCS helpers.
# SPCS variables are filled in at the SPCS step (image repository URL from
# `SHOW IMAGE REPOSITORIES`).

IMAGE      ?= bank-app
TAG        ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
REPO_URL   ?=
CONNECTION ?= zs28104-svc_mdaeppen

.PHONY: dev build-local run-local run-readonly stop logs test typecheck e2e build push

dev:            ## Next.js dev server on :8080 (uses ~/.snowflake directly)
	npm run dev

build-local:    ## linux/arm64 image for Docker Desktop (same arch as SPCS)
	docker compose build

run-local:      ## Run the container (HOST_PORT=8088 if 8080 is taken)
	docker compose up -d && docker compose ps

run-readonly:   ## Second container as DTI_READ_ONLY on :8089 (governance e2e: RO_URL=http://localhost:8089)
	HOST_PORT=8089 SNOWFLAKE_ROLE=DTI_READ_ONLY docker compose -p bank-app-ro up -d --no-build

stop:
	docker compose down

logs:
	docker compose logs -f bank-app

typecheck:
	npx tsc --noEmit

test:
	npm test

e2e:            ## Playwright against a running app (BASE_URL, default :8080)
	npx playwright test

build:          ## linux/arm64 image for SPCS (ARM compute pool)
	docker buildx build --platform linux/arm64 -t $(IMAGE):$(TAG) .

push:           ## Push to the Snowflake image repository
	@test -n "$(REPO_URL)" || (echo "Set REPO_URL=<image repository url>" && exit 1)
	snow spcs image-registry login -c $(CONNECTION)
	docker tag $(IMAGE):$(TAG) $(REPO_URL)/$(IMAGE):$(TAG)
	docker push $(REPO_URL)/$(IMAGE):$(TAG)
