import sys
import asyncio
from uuid import uuid4
import subprocess
import random
import string
import aioredis
from aioredis import RedisError
import time
import threading
import matplotlib.pyplot as plt

# Redis server address
REDIS_HOME_HOST = "10.0.0.254"
REDIS_PORT = 6379
CHANNEL = "cache_updates"

startup_nodes = [
  {"host": REDIS_HOME_HOST, "port": REDIS_PORT},
]

# Cache application with MESI protocol states and invalidation
class CacheApp:
    def __init__(self, host_name):
        self.host_name = host_name
        self.loop = asyncio.get_event_loop()  # Get event loop
        self.cache = None
        self.pub_sub = None
        self.read_probability = 0.8  # Default read probability (80%)
        self.cache_state = "I"  # Initial state: Invalid
        self.write_latencies = []  # Stores write latencies
        self.read_latencies = []  # Stores read latencies
        self.should_stop = False  # stop flag

    async def connect_to_redis(self):
        try:
            self.cache = await aioredis.Redis(host="127.0.0.1", port=REDIS_PORT)
            self.pub_sub = self.cache.pubsub()
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
        start_time = time.time()  # Start time for read latency
        async with aioredis.Redis(host="localhost", port=REDIS_PORT) as local_cache:
            # Check cache state and received updates
            if self.cache_state == "M":  # Modified - dirty
                data = await local_cache.get(key)
                if not data:
                    await self.publish_invalidate(key)
                    self.cache_state = "I"
                    print(f"{self.host_name}: Data not found and invalidated for {key}")
                    return None
                return data
            elif self.cache_state == "S":  # Shared - clean
                return await local_cache.get(key)
            else:  # I or E here - data is invalid or exclusive (get from home manager)
                data = await self.fetch_from_home_manager(key)
                if data:
                    self.cache_state = "S"  # --> Shared
                    await local_cache.set(key, data.encode())
                    return data
                else:
                    if self.cache_state == "S":
                        await self.publish_invalidate(key)
                        self.cache_state = "I"
                        print(f"{self.host_name}: Data not found and invalidated for {key}")
                        return None
        self.read_latencies.append(time.time() - start_time)  # Add read latency
        return data

    async def fetch_from_home_manager(self, key):
        async with aioredis.Redis(host=REDIS_HOME_HOST, port=REDIS_PORT) as home_manager_cache: # connection to home
            try:
                data = await home_manager_cache.get(key)
                if data:
                    print(f"{self.host_name}: Retrieved data from home manager for {key}")
                    return data
                else:
                    # Data not found on home manager (invalidate locally)
                    self.cache_state = "I"
                    return None
            except RedisError as e:
                print(f"{self.host_name}: Error retrieving data from home manager ({key}): {e}")
                return None

    async def set_data(self, key):
        start_time = time.time()  # Start time for write latency
        # Update logic based on cache state
        if self.cache_state == "I":  # Invalid - fetch data and --> to S
            data = await self.fetch_from_home_manager(key)
            if data:
                await self.cache.set(key, data.encode())
                self.cache_state = "S"
        elif self.cache_state == "S":  # Shared --> to M and send update
            new_value = self.gen_random_data()  # Simulate data modification
            await self.cache.set(key, new_value.encode())
            self.cache_state = "M"
            await self.publish_update(key, new_value)
            # Invalidate other caches (write-invalidate)
            await self.publish_invalidate(key)
            print(f"{self.host_name}: Updated data for {key} (invalidated others)")
        elif self.cache_state == "M":  # Modified - update locally (don't fetch)
            new_value = self.gen_random_data()
            await self.cache.set(key, new_value.encode())
            await self.publish_update(key, new_value)
            
        write_latency = time.time() - start_time
        self.write_latencies.append(write_latency)

    async def publish_update(self, key, value):
        message = f"UPDATE|{key}|{value}"
        await self.cache.publish(CHANNEL, message.encode())

    async def publish_invalidate(self, key):
        message = f"INVALIDATE|{key}"
        await self.cache.publish(CHANNEL, message.encode())

    async def listen_for_updates(self):
        while True:
            async for message in self.pub_sub.listen():
                if message['type'] == 'message':
                    data = message['data'].decode().split('|')
                    command, key, value = data
                    if command == "UPDATE":
                        # Store update
                        self.received_update = {"key": key, "value": value}
                        print(f"{self.host_name}: Stored update for {key}")
                    elif command == "INVALIDATE":
                        # Invalidate local cache for the key
                        self.cache_state = "I"
                        await self.cache.delete(key)
                        print(f"{self.host_name}: Received invalidate for {key}")

    async def run_simulation(self):
        while self.should_stop == False:
            # Choose a random key to access
            key = f"data_{random.randint(1, 100)}"

            await asyncio.sleep(random.uniform(0.1, 0.5))  # Add some Random sleep

            # Retrieve data using get_data
            await self.get_data(key)
            print(f"{self.host_name}: Retrieved data for key {key}")

            if random.random() < 0.2:
                await self.set_data(key)
                print(f"{self.host_name}: Updated data for key {key}")    
            # Graphing
            avg_read_latency = sum(self.read_latencies) / len(self.read_latencies)
            avg_write_latency = sum(self.write_latencies) / len(self.write_latencies)

            # Plot read latency
            plt.figure(figsize=(8, 6))
            plt.plot(self.read_latencies, label="Read Latency")
            plt.xlabel("Time Step")
            plt.ylabel("Read Latency (s)")
            plt.title(f"{self.host_name} - Read Latencies (Average: {avg_read_latency:.4f}s)") 
            plt.legend()
            plt.grid(True)
            plt.savefig(f"{self.host_name}_read_latencies.png")  # Save plot as an image

            # Plot write latency
            plt.figure(figsize=(8, 6))
            plt.plot(self.write_latencies, label="Write Latency")
            plt.xlabel("Time Step")
            plt.ylabel("Write Latency (s)")
            plt.title(f"{self.host_name} - Write Latencies (Averageg: {avg_write_latency:.4f}s)")
            plt.legend()
            plt.grid(True)
            plt.savefig(f"{self.host_name}_write_latencies.png")  # Save plot as an image



def stop(self):
    self.should_stop = True  # True stops the event loop


async def run_event_loop(app):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.connect_to_redis())

    task = loop.create_task(app.listen_for_updates())

    await loop.run_until_complete(app.run_simulation())

    # Stop the event loop after simulation finishes
    app.stop()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        read_probability = float(sys.argv[1])
        ip = subprocess.check_output(["hostname", "-I"]).decode()
        app = CacheApp(host_name=f"{ip}")
        app.read_probability = read_probability

        # Start the event loop in another thread
        thread = threading.Thread(target=run_event_loop, args=(app,))
        thread.start()
    else:
        print("Usage: python cache_app.py <read_probability>")