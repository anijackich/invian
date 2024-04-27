import json
import asyncio
from os import getenv
from os.path import exists
from dotenv import load_dotenv
from websockets.server import serve

from invian import InvianStream
from invian.geo import OffsetFilter

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


async def stream_road(websocket, path):
    for snapshot in invian.get_stream():
        await websocket.send(json.dumps(snapshot.to_dict()))
        await asyncio.sleep(0.03)


async def main():
    async with serve(stream_road, WEBSOCKETS_HOST, WEBSOCKETS_PORT):
        await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())
