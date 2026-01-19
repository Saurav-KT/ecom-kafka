COMPOSE = docker compose

COMPOSE_FILES = \
	-f infra/kafka/docker-compose.kafka.yml \
	-f infra/database/docker-compose.postgres.yml \
	-f infra/cache/docker-compose.redis.yml \
	-f infra/search/docker-compose.elasticsearch.yml

infra-up:
	$(COMPOSE) $(COMPOSE_FILES) up -d

infra-down:
	$(COMPOSE) $(COMPOSE_FILES) down

infra-logs:
	$(COMPOSE) $(COMPOSE_FILES) logs -f

infra-restart:
	$(COMPOSE) $(COMPOSE_FILES) restart
