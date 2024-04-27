from math import sin, cos, pi
from pyproj.transformer import Transformer


def utm_to_gps(utm_coords):
    return Transformer.from_proj(
        '+proj=utm +zone=39 +ellps=WGS84',
        '+proj=latlong +ellps=WGS84'
    ).transform(utm_coords[1], utm_coords[0])[::-1]


class OffsetFilter:
    def __init__(self,
                 x_offset: float = 0.,
                 y_offset: float = 0.,
                 rotation_angle: float = 0.,
                 rotation_origin: tuple[float, float] = (0, 0)):
        self.x_offset = x_offset
        self.y_offset = y_offset
        self.rotation_angle = rotation_angle
        self.rotation_origin = rotation_origin

    def _rotate(self, point: tuple[float, float]) -> tuple[float, float]:
        rad_angle = self.rotation_angle * pi / 180
        return (
            self.rotation_origin[0] +
            cos(rad_angle) * (point[0] - self.rotation_origin[0]) -
            sin(rad_angle) * (point[1] - self.rotation_origin[1]),
            self.rotation_origin[1] +
            sin(rad_angle) * (point[0] - self.rotation_origin[0]) -
            cos(rad_angle) * (point[1] - self.rotation_origin[1])
        )

    def _move(self, point: tuple[float, float]) -> tuple[float, float]:
        return point[0] + self.x_offset, point[1] + self.y_offset

    def offset(self, point: tuple[float, float]) -> tuple[float, float]:
        return self._move(self._rotate(point))
