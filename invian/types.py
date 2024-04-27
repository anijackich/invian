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
    coords: tuple[int, int]
    transport_type: Transport

    def __hash__(self):
        return hash((self.coords, self.transport_type))


@dataclass
class RoadSnapshot:
    timestamp: int
    cars: set[Car]


@dataclass
class _RawMessage:
    unix_millis: int
    center: tuple[int, int]
    cls: Transport

    def __init__(self, data: dict):
        self.unix_millis = data['unix_millis']
        self.center = tuple(data['center'])
        self.cls = Transport(data['class'])


@dataclass
class Metrics:
    car_types: dict[int, int]
    total_cars: int
