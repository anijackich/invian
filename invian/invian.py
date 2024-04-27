import json
from typing import Iterator
from kafka import KafkaConsumer

from .utils import utm_to_gps
from .types import RoadSnapshot, Car, _RawMessage


class InvianStream:
    def __init__(self, server: str, group_id: str, topics: list[str]):
        self.kafka_consumer = KafkaConsumer(*topics,
                                            bootstrap_servers=server,
                                            group_id=group_id)

    def __del__(self):
        self.kafka_consumer.close()

    def _get_raw_stream(self):
        return (
            _RawMessage(json.loads(msg.value.decode()))
            for msg in self.kafka_consumer
        )

    def get_stream(self, buffer_length: int = 4) -> Iterator[RoadSnapshot]:
        buffer = {}

        current_ts = 0
        current_msgs = []

        for msg in self._get_raw_stream():
            if msg.unix_millis != current_ts:
                buffer[current_ts] = buffer[current_ts] + current_msgs \
                    if current_ts in buffer else current_msgs
                current_msgs.clear()
                current_ts = msg.unix_millis

            current_msgs.append(msg)

            if len(buffer) >= buffer_length:
                msgs = buffer.pop(min(buffer.keys()))
                yield RoadSnapshot(
                    timestamp=msgs[0].unix_millis,
                    cars=set(
                        Car(coords=utm_to_gps(msg.center),
                            transport_type=msg.cls) for msg in msgs
                    )
                )
