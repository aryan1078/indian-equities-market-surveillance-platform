from __future__ import annotations

from kafka import KafkaConsumer, KafkaProducer

from .serialization import dumps
from .settings import get_settings


def build_producer() -> KafkaProducer:
    settings = get_settings()
    return KafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers, value_serializer=dumps)


def build_consumer(topic: str, group_id: str) -> KafkaConsumer:
    settings = get_settings()
    return KafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=group_id,
        value_deserializer=lambda value: value,
        key_deserializer=lambda value: value.decode("utf-8") if value else None,
    )
