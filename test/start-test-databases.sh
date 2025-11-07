#!/bin/bash
# Script to start test databases (Redis and Virtuoso) for oc_meta

# Get the absolute path of the script's directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# Use the script's directory to ensure relative paths are correct
TEST_DIR="$SCRIPT_DIR"
PROJECT_ROOT="$(dirname "$TEST_DIR")"

# Create test database directories if they don't exist
mkdir -p "${TEST_DIR}/test_virtuoso_db"
mkdir -p "${TEST_DIR}/test_virtuoso_prov_db"

REDIS_CONTAINER="oc-meta-test-redis"
VIRTUOSO_CONTAINER="oc-meta-test-virtuoso"
VIRTUOSO_PROV_CONTAINER="oc-meta-test-virtuoso-prov"

echo "Checking for existing containers..."
if [ "$(docker ps -a -q -f name=$VIRTUOSO_CONTAINER)" ]; then
    echo "Removing existing $VIRTUOSO_CONTAINER container..."
    docker rm -f $VIRTUOSO_CONTAINER
fi

if [ "$(docker ps -a -q -f name=$VIRTUOSO_PROV_CONTAINER)" ]; then
    echo "Removing existing $VIRTUOSO_PROV_CONTAINER container..."
    docker rm -f $VIRTUOSO_PROV_CONTAINER
fi

if [ "$(docker ps -a -q -f name=$REDIS_CONTAINER)" ]; then
    echo "Removing existing $REDIS_CONTAINER container..."
    docker rm -f $REDIS_CONTAINER
fi

echo "Starting $VIRTUOSO_CONTAINER container (data)..."
docker run -d --name $VIRTUOSO_CONTAINER \
  -p 8805:8890 \
  -p 1105:1111 \
  -e DBA_PASSWORD=dba \
  -e SPARQL_UPDATE=true \
  -v "${TEST_DIR}/test_virtuoso_db:/database" \
  openlink/virtuoso-opensource-7:7.2.15

echo "Starting $VIRTUOSO_PROV_CONTAINER container (provenance)..."
docker run -d --name $VIRTUOSO_PROV_CONTAINER \
  -p 8806:8890 \
  -p 1106:1111 \
  -e DBA_PASSWORD=dba \
  -e SPARQL_UPDATE=true \
  -v "${TEST_DIR}/test_virtuoso_prov_db:/database" \
  openlink/virtuoso-opensource-7:7.2.15

echo "Starting $REDIS_CONTAINER container..."
docker run -d --name $REDIS_CONTAINER \
  -p 6381:6379 \
  redis:7-alpine

echo "Waiting for test databases to start (approx. 30 seconds)..."
sleep 30
echo "Test databases should be ready now."

if ! docker ps -q -f name=$VIRTUOSO_CONTAINER > /dev/null; then
  echo "Error: $VIRTUOSO_CONTAINER failed to start."
  docker logs $VIRTUOSO_CONTAINER
  exit 1
fi

if ! docker ps -q -f name=$VIRTUOSO_PROV_CONTAINER > /dev/null; then
  echo "Error: $VIRTUOSO_PROV_CONTAINER failed to start."
  docker logs $VIRTUOSO_PROV_CONTAINER
  exit 1
fi

echo "Assigning SPARQL_UPDATE role to the SPARQL account in $VIRTUOSO_CONTAINER..."
docker exec $VIRTUOSO_CONTAINER /opt/virtuoso-opensource/bin/isql -U dba -P dba exec="DB.DBA.USER_GRANT_ROLE ('SPARQL', 'SPARQL_UPDATE');"

if [ $? -ne 0 ]; then
    echo "Error: Failed to grant SPARQL_UPDATE role in $VIRTUOSO_CONTAINER."
    docker logs $VIRTUOSO_CONTAINER
    exit 1
fi

echo "Setting default RDF user permissions (read/write) in $VIRTUOSO_CONTAINER..."
docker exec $VIRTUOSO_CONTAINER /opt/virtuoso-opensource/bin/isql -U dba -P dba exec="DB.DBA.RDF_DEFAULT_USER_PERMS_SET ('nobody', 7);"

if [ $? -ne 0 ]; then
    echo "Error: Failed to set default RDF user permissions in $VIRTUOSO_CONTAINER."
    docker logs $VIRTUOSO_CONTAINER
    exit 1
fi

echo "Assigning SPARQL_UPDATE role to the SPARQL account in $VIRTUOSO_PROV_CONTAINER..."
docker exec $VIRTUOSO_PROV_CONTAINER /opt/virtuoso-opensource/bin/isql -U dba -P dba exec="DB.DBA.USER_GRANT_ROLE ('SPARQL', 'SPARQL_UPDATE');"

if [ $? -ne 0 ]; then
    echo "Error: Failed to grant SPARQL_UPDATE role in $VIRTUOSO_PROV_CONTAINER."
    docker logs $VIRTUOSO_PROV_CONTAINER
    exit 1
fi

echo "Setting default RDF user permissions (read/write) in $VIRTUOSO_PROV_CONTAINER..."
docker exec $VIRTUOSO_PROV_CONTAINER /opt/virtuoso-opensource/bin/isql -U dba -P dba exec="DB.DBA.RDF_DEFAULT_USER_PERMS_SET ('nobody', 7);"

if [ $? -ne 0 ]; then
    echo "Error: Failed to set default RDF user permissions in $VIRTUOSO_PROV_CONTAINER."
    docker logs $VIRTUOSO_PROV_CONTAINER
    exit 1
fi

echo "Permissions set successfully in Virtuoso containers."
echo "Virtuoso Data SPARQL Endpoint: http://localhost:8805/sparql"
echo "Virtuoso Data ISQL Port: 1105"
echo "Virtuoso Provenance SPARQL Endpoint: http://localhost:8806/sparql"
echo "Virtuoso Provenance ISQL Port: 1106"
echo "Redis: localhost:6381" 