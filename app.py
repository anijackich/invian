import json
import asyncio
from os import getenv
from os.path import exists
from dotenv import load_dotenv
from websockets.server import serve

from invian import InvianStream
from invian.geo import OffsetFilter
from invian.types import RoadSnapshot, RoadMetrics

load_dotenv()

KAFKA_HOST, KAFKA_PORT, KAFKA_GROUP, KAFKA_TOPIC = (getenv('KAFKA_HOST'),
                                                    getenv('KAFKA_PORT'),
                                                    getenv('KAFKA_GROUP'),
                                                    getenv('KAFKA_TOPIC'))

WEBSOCKETS_HOST, WEBSOCKETS_PORT = (getenv('WEBSOCKETS_HOST'),
                                    int(getenv('WEBSOCKETS_PORT')))

if exists('offset_conf.json'):
    with open('offset_conf.json') as fp:
        offset_filter = OffsetFilter(**json.load(fp))
else:
    offset_filter = OffsetFilter()

invian = InvianStream(
    server=f"{KAFKA_HOST}:{KAFKA_PORT}",
    group_id=KAFKA_GROUP,
    topics=[KAFKA_TOPIC],
    offset_filter=offset_filter
)

num_consumers = 2
queues = [asyncio.Queue() for _ in range(num_consumers)]


async def distribute_messages(stream):
    global queues

    for msg in stream:
        for q in queues:
            await q.put(msg)
        await asyncio.sleep(0.03)


async def stream_road(websocket, _):
    global queues

    while True:
        snapshot = await queues[0].get()
        await websocket.send(json.dumps(snapshot.to_dict()))
        await asyncio.sleep(0.03)


async def stream_metrics(websocket, _):
    global queues

    cur_timestamp = 0
    interval = 500  # millisecond

    cars_cnt = 0
    frames = 0

    previous_metric = RoadMetrics(cur_timestamp, 0)
    while True:
        snapshot: RoadSnapshot = await queues[1].get()

        if snapshot.timestamp - cur_timestamp > interval:
            avg_cars = int(cars_cnt / frames) if frames != 0 else 0
            previous_metric = RoadMetrics(cur_timestamp, avg_cars)

            cur_timestamp = snapshot.timestamp
            cars_cnt = 0
            frames = 0

        cars_cnt += len(snapshot.cars)
        frames += 1

        await websocket.send(json.dumps(previous_metric.to_dict()))
        await asyncio.sleep(0.03)


async def main():
    road_socket = serve(stream_road, WEBSOCKETS_HOST, WEBSOCKETS_PORT)
    metrics_socket = serve(stream_metrics, WEBSOCKETS_HOST, WEBSOCKETS_PORT + 10)
    await asyncio.gather(distribute_messages(invian.get_stream()), road_socket, metrics_socket)


if __name__ == '__main__':
    asyncio.run(main())
