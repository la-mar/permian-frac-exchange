COMMIT_HASH    := $$(git log -1 --pretty=%h)
DATE := $$(date +"%Y-%m-%d")
CTX:=.
AWS_ACCOUNT_ID:=$$(aws-vault exec ${ENV} -- aws sts get-caller-identity | jq .Account -r)
IMAGE_NAME:=driftwood/fsec
DOCKERFILE:=Dockerfile
ENV:=prod

cc-expand:
	# show expanded configuration
	circleci config process .circleci/config.yml

cc-process:
	circleci config process .circleci/config.yml > process.yml

cc-run-local:
	JOBNAME?=build-image
	circleci local execute -c process.yml --job build-image -e DOCKER_LOGIN=${DOCKER_LOGIN} -e DOCKER_PASSWORD=${DOCKER_PASSWORD}

run-tests:
	pytest --cov=fsec tests/ --cov-report xml:./coverage/python/coverage.xml --log-cli-level debug

smoke-test:
	docker run --entrypoint fsec driftwood/fsec:${COMMIT_HASH} test smoke-test

cov:
	pytest --cov fsec --cov-report html:./coverage/coverage.html --log-level info --log-cli-level debug

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
	docker build  -f ${DOCKERFILE} ${CTX} -t ${IMAGE_NAME}
	docker tag ${IMAGE_NAME} ${IMAGE_NAME}:${COMMIT_HASH}

push: login
	docker push ${IMAGE_NAME}

all:
	make build login push

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
	aws-vault exec prod -- docker run -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN -e AWS_SECURITY_TOKEN -e LOG_FORMAT driftwood/fsec fsec run collector
