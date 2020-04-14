COMMIT_HASH    := $$(git log -1 --pretty=%h)
DATE := $$(date +"%Y-%m-%d")
CTX:=.
AWS_ACCOUNT_ID:=$$(aws-vault exec ${ENV} -- aws sts get-caller-identity | jq .Account -r)
IMAGE_NAME:=driftwood/fracx
DOCKERFILE:=Dockerfile
ENV:=prod
APP_VERSION ?= $$(grep -o '\([0-9]\+.[0-9]\+.[0-9]\+\)' pyproject.toml | head -n1)

cc-expand:
	# show expanded configuration
	circleci config process .circleci/config.yml

cc-process:
	circleci config process .circleci/config.yml > process.yml

cc-run-local:
	JOBNAME?=build-image
	circleci local execute -c process.yml --job build-image -e DOCKER_LOGIN=${DOCKER_LOGIN} -e DOCKER_PASSWORD=${DOCKER_PASSWORD}

run-tests:
	pytest --cov src/fracx tests/ --cov-report xml:./coverage/python/coverage.xml --log-cli-level debug

smoke-test:
	docker run --entrypoint fracx driftwood/fracx:${COMMIT_HASH} test smoke-test

lint:
	flake8 ./src --max-line-length=88 --extend-ignore=E203

cov:
	pytest --cov src/fracx tests/ --cov-report html:./coverage/coverage.html --log-level info --log-cli-level info

pcov:
	pytest --cov src/fracx tests/


view-cov:
	open -a "Google Chrome" ./coverage/coverage.html/index.html

release:
	poetry run python scripts/release.py

export-deps:
	poetry export -f requirements.txt > requirements.txt --without-hashes

login:
	docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD}

build:
	@echo "Building docker image: ${IMAGE_NAME}"
	docker build  -f Dockerfile . -t ${IMAGE_NAME}
	docker tag ${IMAGE_NAME} ${IMAGE_NAME}:${COMMIT_HASH}
	docker tag ${IMAGE_NAME} ${IMAGE_NAME}:${APP_VERSION}
	docker tag ${IMAGE_NAME} ${IMAGE_NAME}:latest


build-with-chamber:
	@echo "Building docker image: ${IMAGE_NAME} (with chamber)"
	docker build  -f Dockerfile.chamber . -t ${IMAGE_NAME}
	docker tag ${IMAGE_NAME} ${IMAGE_NAME}:chamber-${COMMIT_HASH}
	docker tag ${IMAGE_NAME} ${IMAGE_NAME}:chamber-${APP_VERSION}
	docker tag ${IMAGE_NAME} ${IMAGE_NAME}:chamber-latest

build-all: build-with-chamber build

push: login
	docker push ${IMAGE_NAME}:latest
	docker push ${IMAGE_NAME}:chamber-latest

push-version: build build-with-chamber
	@echo "Pushing images to DockerHub for app version ${APP_VERSION}"
	docker push ${IMAGE_NAME}:${APP_VERSION}
	docker push ${IMAGE_NAME}:chamber-${APP_VERSION}

all: build-all login push

deploy:
	aws-vault exec ${ENV} -- poetry run python scripts/deploy.py

redeploy:
	aws ecs update-service --cluster ${ECS_CLUSTER} --service ${SERVICE_NAME} --force-new-deployment --profile ${ENV}


ssm-export:
	# Export all SSM parameters associated with this service to json
	aws-vault exec ${ENV} -- chamber export ${SERVICE_NAME} | jq

ssm-export-dotenv:
	# Export all SSM parameters associated with this service to dotenv format
	aws-vault exec ${ENV} -- chamber export --format=dotenv ${SERVICE_NAME} | tee .env.ssm

env-to-json:
	# pipx install json-dotenv
	python3 -c 'import json, os, dotenv;print(json.dumps(dotenv.dotenv_values(".env.production")))' | jq

ssm-update:
	@echo "Updating parameters for ${AWS_ACCOUNT_ID}/${SERVICE_NAME}"
	python3 -c 'import json, os, dotenv; values={k.lower():v for k,v in dotenv.dotenv_values(".env.production").items()}; print(json.dumps(values))' | jq | aws-vault exec ${ENV} -- chamber import ${SERVICE_NAME} -

view-credentials:
	# print the current temporary credentials from aws-vault
	aws-vault exec ${ENV} -- env | grep AWS


secret-key:
	python3 -c 'import secrets; print(secrets.token_urlsafe(256));'


docker-run-collector:
	docker run --env-file .env.production driftwood/fracx:latest fracx run collector


docker-run-collector-with-chamber:
	aws-vault exec prod -- docker run -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN -e AWS_SECURITY_TOKEN -e LOG_FORMAT driftwood/fracx:latest-chamber fracx run collector



put-rule-10pm:
	aws events put-rule --schedule-expression "cron(0 22 * * ? *)" --name schedule-10pm
