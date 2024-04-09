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

sys.setrecursionlimit(5000)

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
        self.home = None
        self.pub_sub = None
        self.read_probability = 0.8 # Default read probability (80%)
        self.layers_traversed = 1
        self.cache_state = "I"  # Initial state: Invalid
        self.write_latencies = []
        self.read_latencies = []  
        self.is_last_node = False
        self.sim_time = 10

    async def stop_event_loop(self):
        app.write_read_latency(app.read_latencies)
        app.write_write_latency(app.write_latencies)
        await self.pub_sub.unsubscribe()
        # await self.home.client_kill(f"{REDIS_HOME_HOST}:{REDIS_PORT}")
        await self.home.close()
        #print(f"Kill signal for {app.host_name}. Sending latencies...")
        os._exit(1)
    
    async def connect_to_redis(self, max_retries):
        tries = 0
        delay = 0.5
        while tries < max_retries:
            try:
                self.cache = await aioredis.Redis(host="127.0.0.1", port=REDIS_PORT)
                self.pool = aioredis.BlockingConnectionPool(host=REDIS_HOME_HOST, port=REDIS_PORT, health_check_interval=30, max_connections=500)
                self.home = await aioredis.Redis(connection_pool=self.pool)
                self.pub_sub = self.home.pubsub()
                await self.pub_sub.subscribe(CHANNEL)
                #print(f"{self.host_name}: Connected to Redis successfully")
                break
            except RedisError as e:
                print(f"Connection error: {e}. Retrying in {delay} seconds...")
                tries += 1
            await asyncio.sleep(delay)
        if (tries == max_retries):
            raise ConnectionError("Failed to connect to Redis after retries")

    def gen_random_data(self):
        data = ''
        for _ in range(10):
            data += random.choice(string.ascii_letters + string.digits)
        return data

    def write_latency_to_file(self, latency_type, values):
        filename = f"{latency_type}_{self.host_name}.txt"
        filepath = os.path.join(f"./latencies2/{str(self.layers_traversed)}/{str(self.read_probability)}/", filename)
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "a") as f:
                for val in values:
                    f.write(f"{val}\n")
        except (IOError, OSError) as e:
            print(f"Error writing to file: {e}")

    def write_read_latency(self, latency):
        self.write_latency_to_file("read", latency)

    def write_write_latency(self, latency):
        self.write_latency_to_file("write", latency)

    async def get_data(self, key):
        start_time = time.time()  # Start time for read latency
        try:
            data = await self.get_from_home_manager(key)
            if data:
                self.read_latencies.append(time.time() - start_time)
        except:
            await asyncio.sleep(5)
            await self.connect_to_redis(40)

    async def get_from_home_manager(self, key):
        try:
            # Fetch all keys from the Redis cache
            keys = await self.home.keys('*')

            # Fetch values for each key
            values = []
            for key in keys:
                value = await self.home.get(key)
                values.append(value.decode('utf-8'))

            # print(f"all home keys: {keys}")
            # print(f"all home values: {values}")
            # #

            data = await self.home.get(key)
            if data:
                # print(f"{self.host_name}: Retrieved data from home manager for {key.decode('utf-8')}")
                return data
            else:
                # Data not found on home manager
                # print(f"{self.host_name}: Data not in home manager for {key}, setting data")
                new_value = self.gen_random_data()
                await self.home.set(key, new_value)
                self.cache_state = "S"
                return new_value
        except:
            # print(f"{self.host_name}: Error retrieving data from home manager ({key}): {e}")
            await asyncio.sleep(5)
            await self.connect_to_redis(40)

    async def set_data(self, key):
        start_time = time.time()  # Start time for write latency
        try:
            data = await self.get_from_home_manager(key)
            if data:
                self.write_latencies.append(time.time() - start_time)
        except:
            await asyncio.sleep(5)
            await self.connect_to_redis(40)
    
    async def publish_terminate(self):
        message = "TERMINATE"
        try:
            subscribers = await self.home.publish(CHANNEL, message)
            print(f"subscribers: {subscribers}")
            return subscribers
        except:
            await asyncio.sleep(5)
            await self.connect_to_redis(40)

    async def listen_for_updates(self):
        while True:
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
                await self.connect_to_redis(40)

    async def run_simulation(self):
        if self.is_last_node == False:
            while True:
                # random key to access
                key = f"data_{random.randint(1, 10)}"
                
                await asyncio.sleep(random.uniform(0.1, 0.5))  # Add some Random sleep)

                if random.random() < 1 - self.read_probability: # write
                    await self.set_data(key)
                    # print(f"{self.host_name}: Updated data for key {key}")    
                else: # read
                    await self.get_data(key)
                    # print(f"{self.host_name}: Retrieved data for key {key}")
        else:
            start_time = time.time()
            while time.time() < self.sim_time + start_time:
                # random key to access
                key = f"data_{random.randint(1, 10)}"
                
                await asyncio.sleep(random.uniform(0.1, 0.5))  # Add some Random sleep)

                if random.random() < 1 - self.read_probability: # write
                    await self.set_data(key)
                    print(f"{self.host_name}: Updated data for key {key}")    
                else: # read
                    await self.get_data(key)
                    print(f"{self.host_name}: Retrieved data for key {key}")        
            while (await self.publish_terminate() > 5):
                await asyncio.sleep(1)
            await self.stop_event_loop()
        
async def run_event_loop(app):
    loop = asyncio.get_event_loop()
    await app.connect_to_redis(40)

    simulation_task = loop.create_task(app.run_simulation())
    update_task = loop.create_task(app.listen_for_updates())

    await asyncio.gather(simulation_task, update_task)

if __name__ == "__main__":
    if len(sys.argv) == 6:
        app = CacheApp(sys.argv[1])
        app.read_probability = float(sys.argv[2])
        app.layers_traversed = int(sys.argv[3])
        app.is_last_node = eval(sys.argv[4])
        app.sim_time = int(sys.argv[5])
        asyncio.run(run_event_loop(app)) # run in main thread
    else:
        print("Usage: python cache_app.py <read_probability>")