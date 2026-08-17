#!/usr/bin/env bash
set -e

echo "========================================="
echo " Building Docker images for Microservices "
echo "========================================="

echo "Building user-service image..."
docker build -t local/user-service:latest ./src/user-service

echo "Building wallet-service image..."
docker build -t local/wallet-service:latest ./src/wallet-service

echo "Building betting-service image..."
docker build -t local/betting-service:latest ./src/betting-service

echo "Building events-service image..."
docker build -t local/events-service:latest ./src/events-service

echo "Building management-service image..."
docker build -t local/management-service:latest ./src/management-service

echo "Building notification-service image..."
docker build -t local/notification-service:latest ./src/notification-service

echo "Building mock-service image..."
docker build -t local/mock-service:latest ./src/mock-service

echo "========================================="
echo " All Docker images built successfully!   "
echo "========================================="
