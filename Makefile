# Makefile - common commands for development.

DJANGO_CONTAINER_NAME = web
DJANGO_SERVICE_NAME = web
DATABASE_SERVICE_NAME = postgres
PGADMIN_SERVICE_NAME = pgadmin
REDIS_SERVICE_NAME = redis
REDIS_COMMANDER_SERVICE_NAME = redis-commander
JUPYTER_NOTEBOOK_SERVICE_NAME = notebook
MAIL_HOG_SERVICE_NAME = mailhog
BEAT_SERVICE_NAME = beat
WORKER_SERVICE_NAME = worker
FLOWER_SERVICE_NAME = flower

## -- docker targets --

init:
	docker exec -it ${DJANGO_CONTAINER_NAME} ./scripts/init_data.sh

ps:
	docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Ports}}\t{{.Status}}'

attach:
	docker exec -it ${DJANGO_CONTAINER_NAME} bash

shell:
	docker exec -it ${DJANGO_CONTAINER_NAME} python manage.py shell


## -- docker-compose targets --

## validate file
config:
	docker-compose config

## run postgres service
up-postgres:
	docker-compose -f docker-compose.prod.yml up ${DATABASE_SERVICE_NAME} -d

## run pgadmin service
up-pgadmin:
	docker-compose -f docker-compose.prod.yml up ${PGADMIN_SERVICE_NAME} -d

## run redis service
up-redis:
	docker-compose -f docker-compose.dev.yml up ${REDIS_SERVICE_NAME} -d

## run web service
up-redis-commander:
	docker-compose -f docker-compose.prod.yml up ${REDIS_COMMANDER_SERVICE_NAME} -d

## run web service
up-web:
	docker-compose -f docker-compose.dev.yml up ${DJANGO_SERVICE_NAME} -d

## run web-prod service
up-web-prod:
	docker-compose -f docker-compose.prod.yml up apsvp -d

## run beat service
up-beat:
	docker-compose -f docker-compose.prod.yml up ${BEAT_SERVICE_NAME} -d

## run worker service
up-worker:
	docker-compose -f docker-compose.prod.yml up ${WORKER_SERVICE_NAME} -d

## run flower service
up-flower:
	docker-compose -f docker-compose.prod.yml up ${FLOWER_SERVICE_NAME} -d

## run jupyter service
up-jupyter:
	docker-compose -f docker-compose.prod.yml up ${JUPYTER_NOTEBOOK_SERVICE_NAME} -d

## run mail service
up-mail:
	docker-compose -f docker-compose.dev.yml up ${MAIL_HOG_SERVICE_NAME} -d

## build and run web service, if image source is updated
build-web:
	docker-compose -f docker-compose.dev.yml up --build ${DJANGO_SERVICE_NAME} -d

## docker compose up in development and background
up-dev:
	docker-compose -f docker-compose.dev.yml up -d

down-dev:
	docker-compose -f docker-compose.dev.yml down

## test web service
test:
	docker-compose -f docker-compose.dev.yml run --rm ${DJANGO_SERVICE_NAME} pytest -v

## make migrations in web service
migrate:
	docker-compose -f docker-compose.dev.yml exec ${DJANGO_SERVICE_NAME} python manage.py makemigrations
	docker-compose -f docker-compose.dev.yml exec ${DJANGO_SERVICE_NAME} python manage.py migrate

## create i18n messages in web service
locale:
	docker-compose -f docker-compose.prod.yml exec apsvp python manage.py makemessages -l zh_Hant

## update i18n messages in web service
make-messages:
	docker-compose -f docker-compose.prod.yml exec apsvp django-admin makemessages

## compile i18n messages in web service
compile-messages:
	docker-compose -f docker-compose.dev.yml exec apsvp django-admin compilemessages

## run pytest-cov with web service
pytest-cov:
	docker-compose run ${DJANGO_SERVICE_NAME} pytest --cov-report html --cov=.

## Check to see if the local postgres service service is running
pg-is-ready:
	docker-compose exec ${DATABASE_SERVICE_NAME} pg_isready

## Check to see if the local redis service is running
redis-ping:
	docker-compose exec ${REDIS_SERVICE_NAME} redis-cli ping