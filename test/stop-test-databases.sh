#!/bin/bash
# Script to stop and remove test databases (Redis and Virtuoso) for oc_meta

# Container names
REDIS_CONTAINER="oc-meta-test-redis"
VIRTUOSO_CONTAINER="oc-meta-test-virtuoso"

echo "Stopping and removing test containers..."

if [ "$(docker ps -a -q -f name=$VIRTUOSO_CONTAINER)" ]; then
    echo "Stopping and removing $VIRTUOSO_CONTAINER container..."
    docker stop $VIRTUOSO_CONTAINER > /dev/null
    docker rm $VIRTUOSO_CONTAINER > /dev/null
else
    echo "Container $VIRTUOSO_CONTAINER not found."
fi

if [ "$(docker ps -a -q -f name=$REDIS_CONTAINER)" ]; then
    echo "Stopping and removing $REDIS_CONTAINER container..."
    docker stop $REDIS_CONTAINER > /dev/null
    docker rm $REDIS_CONTAINER > /dev/null
else
    echo "Container $REDIS_CONTAINER not found."
fi

echo "Test containers stopped and removed." 