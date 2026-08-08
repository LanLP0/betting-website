#!/usr/bin/env bash
echo '> docker stack deploy -c docker-compose.yml betting_system'
docker stack deploy -c docker-compose.yml betting_system
