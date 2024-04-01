import asyncio
from uuid import uuid4
import subprocess
import random
import string
import aioredis
from aioredis import RedisError

# Redis server address
REDIS_HOME_HOST = "10.0.0.254"
REDIS_PORT = 6379
CHANNEL = "cache_updates"

startup_nodes = [
  {"host": REDIS_HOME_HOST, "port": REDIS_PORT},
]

# Cache application run by each node using redis pub/sub queue to send updates.
class CacheApp:
    def __init__(self, host_name):
        self.host_name = host_name
        self.loop = asyncio.get_event_loop()  # Get event loop
        self.cache = None
        self.pub_sub = None
        self.read_probability = 0.8 # default 80% read prob

    async def connect_to_redis(self):
        try:
            self.cache = await aioredis.Redis(host="127.0.0.1", port=REDIS_PORT)  # Connect to Redis
            self.pub_sub = self.cache.pubsub()  # Access pubsub from connection
            await self.pub_sub.subscribe(CHANNEL)
            print(f"{self.host_name}: Connected to Redis successfully")
        except RedisError as e:
            print(f"{self.host_name}: Error connecting to Redis: {e}")

    def gen_random_data(self):
        data = ''
        for _ in range(10):
            data += random.choice(string.ascii_letters + string.digits)
        return data

    async def get_data(self, key):
        async with aioredis.Redis(host="localhost", port=REDIS_PORT) as cache:  # Local cache connection
            try:
                data = await cache.get(key)
                if data:
                   print(f"{self.host_name}: Cache hit for {key}")
                   return data
            except RedisError as e:
                print(f"{self.host_name}: Error retrieving data from local cache ({key}): {e}")

            print(f"{self.host_name}: Cache miss for {key}")

            async with aioredis.Redis(host=REDIS_HOME_HOST, port=REDIS_PORT) as home_manager_cache:  # Home manager connection
                try:
                    data = await home_manager_cache.get(key)
                    if data:
                        print(f"{self.host_name}: Retrieved data from home manager for {key}")
                        await cache.set(key, data)  # Store in local cache
                        await self.publish_update(key, data)  # Notify others
                    return data
                except RedisError as e:
                    print(f"{self.host_name}: Error retrieving data from home manager ({key}): {e}")

            print(f"{self.host_name}: Data not found on home manager for {key}")
            data = self.gen_random_data()
            await cache.set(key, data.encode())
            await self.publish_update(key, data)
        return data

    async def set_data(self, key):
    # probability of update (20%) instead of always setting
        if random.random() < self.read_probability:
            new_value = self.gen_random_data()
            try:
                await self.cache.set(key, new_value.encode())
                await self.publish_update(key, new_value)
            except RedisError as e:
                print(f"{self.host_name}: Error setting data in local cache ({key}): {e}")

    async def publish_update(self, key, value):
        message = f"UPDATE|{key}|{value}"
        await self.cache.publish(CHANNEL, message.encode())

    async def publish_invalidate(self, key):
        message = f"INVALIDATE|{key}"
        await self.cache.publish(CHANNEL, message.encode())

    async def listen_for_updates(self):
        while True:
            # Use async for for pub/sub messages
            async for message in self.pub_sub.listen():
                if message['type'] == 'message':
                    data = message['data'].decode().split('|')
                    command, key, value = data
                    if command == "UPDATE":
                        if value:
                            print(data)
                            await self.cache.set(key, value.encode())
                            print(f"{self.host_name}: Received update for {key}")
                        else:
                            print(f"{self.host_name}: Received empty update for {key}")
                    elif command == "INVALIDATE":
                        await self.cache.delete(key)
                        print(f"{self.host_name}: Received invalidate for {key}")
    
    async def run_simulation(self):
        while True:
            # Choose a random key to access
            key = f"data_{random.randint(1, 100)}"

            await asyncio.sleep(random.uniform(0.1, 0.5))  # Add some Random sleep

            # Retrieve data using get_data
            data = await self.get_data(key)
            print(f"{self.host_name}: Retrieved data for key {key}")

            if random.random() < 0.2:
                new_data = self.gen_random_data()
                await self.set_data(key, new_data)
                print(f"{self.host_name}: Updated data for key {key}")

if __name__ == "__main__":
    ip = subprocess.check_output(["hostname", "-I"]).decode()
    app = CacheApp(host_name=f"{ip}")
    # Set read probability to 60% (more reads, less writes)
    app.read_probability = 0.6

    # Connect to Redis and start listening for updates
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.connect_to_redis())

    task = loop.create_task(app.listen_for_updates())

    loop.run_until_complete(app.run_simulation())

    loop.run_forever()