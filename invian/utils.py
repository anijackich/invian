from pyproj.transformer import Transformer


def utm_to_gps(utm_coords):
    return Transformer.from_proj(
        '+proj=utm +zone=39 +ellps=WGS84',
        '+proj=latlong +ellps=WGS84'
    ).transform(utm_coords[1], utm_coords[0])[::-1]
