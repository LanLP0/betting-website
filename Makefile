.PHONY: all build deploy clean

all: build deploy

build:
	@echo "Building all microservice Docker images..."
	./build.sh

deploy:
	@echo "Deploying stack to Docker Swarm..."
	./deploy-swarm.sh

clean:
	@echo "Removing Docker Swarm stack..."
	docker stack rm betting_system || true
