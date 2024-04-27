import json
import asyncio
from os import getenv
from dotenv import load_dotenv
from websockets.server import serve

from invian import Invian

load_dotenv()

invian = Invian(
    server=f"{getenv('KAFKA_HOST')}:{getenv('KAFKA_PORT')}",
    group_id=getenv('KAFKA_GROUP'),
    topics=[getenv('KAFKA_TOPIC')]
)


async def stream_road(websocket, path):
    for snapshot in invian.get_stream():
        await websocket.send(json.dumps(snapshot.to_dict()))
        await asyncio.sleep(0.03)


async def main():
    async with serve(stream_road, "localhost", 8765):
        await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())
