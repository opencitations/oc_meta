#!python
# Copyright (c) 2022 The OpenCitations Index Authors.
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import configparser
import json
import os
from os.path import join

import redis

from oc_meta.datasource.datasource import DataSource

# Read the Redis configuration file
config = configparser.ConfigParser(allow_no_value=True)
cur_path = os.path.dirname(os.path.abspath(__file__))
conf_file = join(cur_path, "config.ini")
config.read(conf_file)


class RedisDataSource(DataSource):
    def __init__(self, service):
        super().__init__(service)
        if service == "DB-META-RA":
            self._r =  redis.Redis(
                            host='127.0.0.1',
                            port=int(config.get('redis', 'port')),
                            db=(config.get('database 0', 'db')),
                            password=None,
                            decode_responses=True
                        )
        elif service == "DB-META-BR":
            self._r = redis.Redis(
                    host='127.0.0.1',
                    port=int(config.get('redis', 'port')),
                    db=(config.get('database 1', 'db')),
                    password=None,
                    decode_responses=True
                )
        else:
            raise ValueError

    def get(self, resource_id):
        redis_data = self._r.get(resource_id)
        if redis_data != None:
            if isinstance(redis_data, str):
                return redis_data
            else:
                return json.loads(redis_data)

    def mget(self, resources_id):
        return {
            resources_id[i]: json.loads(v) if not v is None else None
            for i, v in enumerate(self._r.mget(resources_id))
        }

    def set(self, resource_id, value):
        return self._r.set(resource_id, json.dumps(value))

    def mset(self, resources):
        return self._r.mset({k: json.dumps(v) for k, v in resources.items()})