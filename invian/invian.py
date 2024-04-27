import json
from kafka import KafkaConsumer

from .types import RoadSnapshot, Car, _RawMessage, Metrics


class Invian:
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

    def get_stream(self, buffer_length: int = 4):
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
                        Car(coords=msg.center,
                            transport_type=msg.cls) for msg in msgs
                    )
                )

    def get_metrics(self):
        stream = self.get_stream(4)
        for snap in stream:
            car_types = {}
            for car in snap.cars:
                car_types[car.transport_type.name] = car_types.get(car.transport_type.name, 0) + 1
            total_cars = sum(car_types.values())
            yield Metrics(car_types, total_cars)
