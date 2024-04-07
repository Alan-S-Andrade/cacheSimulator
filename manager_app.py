import sys
import asyncio
import random
import string
import aioredis
from aioredis import RedisError
import time
import signal
import os
import sys

REDIS_PORT = 6379
CHANNEL = "cache_updates"

class Manager:
    def __init__(self):
        self.max_connections = 500
        self.server = None
        self.pool = None
        self.pub_sub = None
        self.loop = asyncio.get_event_loop()
    
    async def create_manager_server(self, max_retries):
        tries = 0
        delay = 0.5
        while tries < max_retries:
            try:
                self.pool = aioredis.BlockingConnectionPool(host="127.0.0.1", port=REDIS_PORT, health_check_interval=30, max_connections=500)
                self.home = await aioredis.Redis(connection_pool=self.pool)
                self.pub_sub = self.home.pubsub()
                await self.pub_sub.subscribe(CHANNEL)
                break
            except RedisError as e:
                print(f"Connection error: {e}. Retrying in {delay} seconds...")
                tries += 1
            await asyncio.sleep(delay)
        if (tries == max_retries):
            raise ConnectionError("Failed to connect to Redis after retries")
    
    async def handle_requests(self):
        while True:
            await asyncio.sleep(random.uniform(0.1, 0.5))  # Add some Random sleep
    
    async def stop_event_loop():
        manager.pool.clear()
        manager.server.close()
        os._exit(1)
    
    async def listen_for_updates(self):
        while self.pub_sub != None:
            try:    
                async for message in self.pub_sub.listen():
                    if message['type'] == 'message':
                        data = message['data'].decode().split('|')
                        command = data[0]
                        if command == "TERMINATE":
                            # print(f"{self.host_name}: Received terminate process")
                            await self.stop_event_loop()
            except aioredis.exceptions.ConnectionError as e:
                print(f"Connection error: {e}")
                await asyncio.sleep(5)
                await self.create_manager_server(40)
        

async def run_event_loop(manager):
    loop = asyncio.get_event_loop()

    simulation_task = loop.create_task(manager.handle_requests())
    update_task = loop.create_task(manager.listen_for_updates())

    await asyncio.gather(simulation_task, update_task)

    
if __name__ == "__main__":
    manager = Manager()
    asyncio.run(run_event_loop(manager))
    

