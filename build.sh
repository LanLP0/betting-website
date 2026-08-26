#!/usr/bin/env bash
set -e

echo "========================================="
echo " Building Docker images for Microservices "
echo "========================================="

echo "Building user-service image..."
docker build -t local/user-service:latest -f ./src/user-service/Dockerfile .

echo "Building wallet-service image..."
docker build -t local/wallet-service:latest -f ./src/wallet-service/Dockerfile .

echo "Building betting-service image..."
docker build -t local/betting-service:latest -f ./src/betting-service/Dockerfile .

echo "Building events-service image..."
docker build -t local/events-service:latest -f ./src/events-service/Dockerfile .

echo "Building management-service image..."
docker build -t local/management-service:latest -f ./src/management-service/Dockerfile .

echo "Building notification-service image..."
docker build -t local/notification-service:latest -f ./src/notification-service/Dockerfile .

echo "Building mock-service image..."
docker build -t local/mock-service:latest -f ./src/mock-service/Dockerfile .

echo "========================================="
echo " All Docker images built successfully!   "
echo "========================================="
