from enum import Enum
from dataclasses import dataclass


class Transport(Enum):
    MOTORCYCLE = 0
    CAR = 1
    CAR_WITH_TRAILER = 2
    TRUCK = 3
    ROAD_TRAIN = 4
    BUS = 5


@dataclass
class Car:
    coords: tuple[float, float]
    transport_type: Transport

    def __hash__(self):
        return hash((self.coords, self.transport_type))

    def to_dict(self):
        return {
            'coords': self.coords,
            'transport_type': self.transport_type.value
        }


@dataclass
class RoadSnapshot:
    timestamp: int
    cars: set[Car]

    def to_dict(self):
        return {
            'timestamp': self.timestamp,
            'cars': list(map(
                lambda car: car.to_dict(), self.cars
            ))
        }


@dataclass
class RoadMetrics:
    timestamp: int
    average_load: int
    transport_types: list[float]

    def to_dict(self):
        return {
            'timestamp': self.timestamp,
            'average_load': self.average_load,
            'transport_types': self.transport_types
        }


@dataclass
class _RawMessage:
    unix_millis: int
    center: tuple[float, float]
    cls: Transport

    def __init__(self, data: dict):
        self.unix_millis = data['unix_millis']
        self.center = tuple(data['center'])
        self.cls = Transport(data['class'])
