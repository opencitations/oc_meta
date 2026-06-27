#!/usr/bin/env sh

# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

set -eu

PORT="${1:-6379}"
CONTAINER_NAME="${2:-oc-meta-redis-${PORT}}"
REDIS_TAG="8.8.0-alpine"
REDIS_DIGEST="sha256:9eb6a7ba3d344e1958c7e1589fa3dee90373a934e8159c634562a91d622759a0"
REDIS_IMAGE="redis:${REDIS_TAG}@${REDIS_DIGEST}"

docker run --rm -d --name "${CONTAINER_NAME}" -p "${PORT}:6379" "${REDIS_IMAGE}"
printf 'Redis is running on localhost:%s in container %s\n' "${PORT}" "${CONTAINER_NAME}"
