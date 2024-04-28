import json
from typing import Iterator
from kafka import KafkaConsumer

from .geo import utm_to_gps, OffsetFilter
from .types import (Car,
                    Transport,
                    RoadSnapshot,
                    RoadMetrics,
                    _RawMessage)


class InvianStream:
    def __init__(self, server: str,
                 group_id: str,
                 topics: list[str],
                 offset_filter: OffsetFilter = OffsetFilter()):
        self.kafka_consumer = KafkaConsumer(*topics,
                                            bootstrap_servers=server,
                                            group_id=group_id,
                                            api_version=(0, 11, 5))

        self.offset_filter = offset_filter

    def _get_raw_stream(self) -> Iterator[_RawMessage]:
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
                        Car(coords=self.offset_filter.offset(utm_to_gps(msg.center)),
                            transport_type=msg.cls) for msg in msgs
                    )
                )

    def get_metrics_stream(self, interval: float = 500) -> Iterator[RoadMetrics]:
        cars, frames = 0, 0
        transport_types = {t.value: 0. for t in Transport}

        current_ts = 0
        current_metric = RoadMetrics(current_ts, 0, transport_types)

        for snapshot in self.get_stream():
            if snapshot.timestamp - current_ts > interval:
                current_metric = RoadMetrics(
                    current_ts,
                    int(cars / frames)
                    if frames != 0 else 0,
                    {t[0]: t[1] / frames for t in transport_types.items()}
                    if frames != 0 else {t.value: 0. for t in Transport}
                )

                current_ts = snapshot.timestamp
                transport_types = {t.value: 0. for t in Transport}
                cars, frames = 0, 0

            for car in snapshot.cars:
                transport_types[car.transport_type.value] += 1

            cars += len(snapshot.cars)
            frames += 1

            yield current_metric
