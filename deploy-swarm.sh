#!/usr/bin/env bash
set -e

echo '> Removing existing Docker Swarm stack (if any)...'
docker stack rm betting_system || true
echo '> Waiting for stack resources to clean up...'
sleep 5

echo '> Deploying betting_system Docker Swarm stack...'
docker stack deploy -c docker-compose.yml betting_system

