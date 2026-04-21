from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg.rows import dict_row
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from redis import Redis

from .settings import get_settings


_cluster: Cluster | None = None
_session = None


def get_cassandra_session():
    global _cluster, _session
    settings = get_settings()
    if _session is None:
        _cluster = Cluster(contact_points=settings.cassandra_contact_points, port=settings.cassandra_port)
        _session = _cluster.connect(settings.cassandra_keyspace)
        _session.row_factory = dict_factory
    return _session


def get_redis() -> Redis:
    return Redis.from_url(get_settings().redis_url, decode_responses=True)


@contextmanager
def pg_connection() -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(get_settings().postgres_dsn, row_factory=dict_row)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()
