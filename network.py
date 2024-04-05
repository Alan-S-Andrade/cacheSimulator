from decimal import Decimal
import numpy as np
from mininet.net import Mininet
from mininet.net import Mininet
from mininet.node import OVSSwitch
from mininet.link import TCLink
from mininet.log import setLogLevel
import time
import multiprocessing as mp
from topology import DynamicTopology
import matplotlib.pyplot as plt
import os
import random

REDIS_PORT = 6379
CHANNEL = "cache_updates"

def configure_cache(net):
  for host in net.hosts:
    host.cmd("echo 'maxmemory 1024' >> /etc/redis/redis.conf")  # Set cache size limit
    host.cmd("echo 'protected-mode no' >> /etc/redis/redis.conf")  # Set cache size limit
    host.cmd("redis-server --daemonize yes")
    host.cmd("redis-cli CONFIG SET protected-mode no")

  # get manager node
  home = net.get('home')
  print(f"Home node IP: {home.IP()}")
  home.cmd("echo 'appendonly yes' >> /etc/redis/redis.conf")  # Enable append-only mode
  home.cmd("echo 'protected-mode no' >> /etc/redis/redis.conf")  # Set cache size limit
  home.cmd("redis-server --daemonize yes")
  home.cmd("redis-cli CONFIG SET protected-mode no")

def set_bandwidth(net, bw):
  print(f"Setting bandwidth to {bw} Mbps")
  for link in net.links:
    link.intf1.config(bw=bw)
    link.intf2.config(bw=bw)

def set_packet_loss(net, loss_percent):
  for link in net.links:
    link.intf1.cmd(f"tc qdisc add dev {link.intf1.name} root netem loss {loss_percent}%")
    link.intf2.cmd(f"tc qdisc add dev {link.intf2.name} root netem loss {loss_percent}%")
  time.sleep(5)

def set_packet_delay(net, delay_time):
  for link in net.links:
    link.intf1.cmd(f"tc qdisc add dev {link.intf1.name} root netem delay {delay_time}ms")
    link.intf2.cmd(f"tc qdisc add dev {link.intf2.name} root netem delay {delay_time}ms")

def remove_hops(topo, net, layer_removed):
  switch_name = topo.all_switches[topo.num_switch_layers - layer_removed]  # remove lowest layers -> upper layers
  print(f"switch_name:{switch_name}")
  switch = net.get(switch_name)
  switch.stop()
  return int(switch_name[1])

def add_hops(topo, net, layer_removed):
  switch_name = topo.all_switches[topo.num_switch_layers - layer_removed]  # remove lowest layers -> upper layers
  switch = net.get(switch_name)
  switch.stop()
  net.waitConnected()

def plot_graphs(all_read_latencies, all_write_latencies):
  # print(all_read_latencies)
  plt.figure(figsize=(10, 6))

  read_probabilities = {}
  for label, latencies in all_read_latencies.items():
    parts = label.split(' - ')
    num_servers = int(parts[0].split(' ')[0])
    read_probability = float(parts[-1].split(' ')[0])
    if read_probability not in read_probabilities:
      read_probabilities[read_probability] = {'num_servers': [], 'average_latencies': []}
    read_probabilities[read_probability]['num_servers'].append(num_servers)
    read_probabilities[read_probability]['average_latencies'].append(np.mean(latencies) * 1000)

  # Plot average latencies for each read probability
  for read_probability, data in read_probabilities.items():
    plt.plot(data['num_servers'], data['average_latencies'], marker='o', label=f'Read Fraction: {read_probability}')

  plt.xlabel('Number of Servers')
  plt.ylabel('Average Read Response Time [ms]')
  plt.legend()
  plt.grid(True)
  plt.savefig('read_response_time_2.pdf')

  ## writes
  plt.figure(figsize=(10, 6))

  write_probabilities = {}
  for label, latencies in all_write_latencies.items():
    parts = label.split(' - ')
    num_servers = int(parts[0].split(' ')[0])
    write_probability = float(parts[-1].split(' ')[0])
    if write_probability not in write_probabilities:
      write_probabilities[write_probability] = {'num_servers': [], 'average_latencies': []}
    write_probabilities[write_probability]['num_servers'].append(num_servers)
    write_probabilities[write_probability]['average_latencies'].append(np.mean(latencies) * 1000)

  # Plot average latencies for each read probability
  for write_probability, data in write_probabilities.items():
    plt.plot(data['num_servers'], data['average_latencies'], marker='o', label=f'Write Fraction: {write_probability}')

  plt.xlabel('Number of Servers')
  plt.ylabel('Average Write Response Time [ms]')
  plt.legend()
  plt.grid(True)
  plt.savefig('write_response_time_2.pdf')

def make_latency_dirs(num_switch_layers, read_probabilities):
  os.mkdir(f'./latencies')
  for layer in range(1, num_switch_layers + 1):
    os.mkdir(f'./latencies/{layer}')
    for read_probability in read_probabilities:
      # make dir for this read_prob run
      os.mkdir(f'./latencies/{layer}/{read_probability}')

def plot_from_files(read_probabilities):
  topo = DynamicTopology()
  topo.create_network()

  all_read_latencies = {}
  all_write_latencies = {}
  none_removed = True

  for layers_traversed in range(1, topo.num_switch_layers + 1):
    for read_probability in read_probabilities:
      read_latencies = []
      write_latencies = []

      for filename in os.listdir(f"./latencies/{layers_traversed}/{read_probability}"):
        if filename.startswith("read"):
          whole_path = os.path.join(f"./latencies/{layers_traversed}/{read_probability}", filename)
          with open(whole_path, "r") as f:
            read_latencies_file = [float(line.strip()) for line in f]
            read_latencies.extend(read_latencies_file)
        elif filename.startswith("write"):
          whole_path = os.path.join(f"./latencies/{layers_traversed}/{read_probability}", filename)
          with open(whole_path, "r") as f:
            write_latencies_file = [float(line.strip()) for line in f]
            write_latencies.extend(write_latencies_file)
      print(f"layerst:{layers_traversed}")
      num_servers_this_layer = topo.all_hosts if none_removed == True else topo.all_hosts - (topo.hosts_per_switch * (layers_traversed - 1))
      print(num_servers_this_layer)
      all_read_latencies[f"{num_servers_this_layer} Servers - {read_probability} Read"] = read_latencies
      all_write_latencies[f"{num_servers_this_layer} Servers - {Decimal('1') - Decimal(str(read_probability))} Write"] = write_latencies
      none_removed = False

  plot_graphs(all_read_latencies, all_write_latencies)


def run(read_probabilities, simulation_time):
  setLogLevel("debug")

  topo = DynamicTopology()
  topo.create_network()

  all_read_latencies = {}
  all_write_latencies = {}

  # Start with all servers active
  net = Mininet(topo=topo, switch=OVSSwitch, waitConnected=True, link=TCLink)
  net.start()
  configure_cache(net)
  make_latency_dirs(topo.num_switch_layers, read_probabilities)

  curr_layer_removed = 0
  layers_removed = 0

  for layers_traversed in range(1, topo.num_switch_layers + 1):
    for read_probability in read_probabilities:
      processes = []

      for i, host in enumerate(net.hosts):
        if host.name != 'home':
          num_hosts_this_layer = (topo.all_hosts - (layers_removed * topo.hosts_per_switch))
          print(num_hosts_this_layer)
          if curr_layer_removed and int(host.name[1]) < curr_layer_removed or curr_layer_removed == 0:
            is_last_node = (i == num_hosts_this_layer) 
            cmd = f"python3 cache_app.py {host.name} {read_probability} {layers_traversed} {is_last_node} {simulation_time}"
            process = mp.Process(target=host.cmd, args=(cmd,))
            processes.append(process)

      # Start all node processes
      for process in processes:
        process.start()
        
      while any(process.is_alive() for process in processes): # wait until all are dead
        time.sleep(0.1)

      read_latencies = []
      write_latencies = []

      for filename in os.listdir(f"./latencies/{layers_traversed}/{read_probability}"):
        if filename.startswith("read"):
          whole_path = os.path.join(f"./latencies/{layers_traversed}/{read_probability}", filename)
          with open(whole_path, "r") as f:
            read_latencies_file = [float(line.strip()) for line in f]
            read_latencies.extend(read_latencies_file)
        elif filename.startswith("write"):
          whole_path = os.path.join(f"./latencies/{layers_traversed}/{read_probability}", filename)
          with open(whole_path, "r") as f:
            write_latencies_file = [float(line.strip()) for line in f]
            write_latencies.extend(write_latencies_file)
        
      num_servers_this_layer = topo.all_hosts if curr_layer_removed == 0 else topo.all_hosts - (topo.hosts_per_switch * layers_traversed)

      all_read_latencies[f"{num_servers_this_layer} Servers - {read_probability} Read"] = read_latencies
      all_write_latencies[f"{num_servers_this_layer} Servers - {Decimal('1') - Decimal(str(read_probability))} Write"] = write_latencies

    print(f"finished {topo.num_switch_layers - layers_traversed} layer execution")
    # Remove nodes by layer (bottom up)
    if topo.num_switch_layers - layers_traversed != 0:
      curr_layer_removed = remove_hops(topo, net, layers_traversed)
    layers_removed += 1

  plot_graphs(all_read_latencies, all_write_latencies)
    
  # bw = random.uniform(0.1, 10)  # random bandwidth between 1 and 10 Mbps
  # set_bandwidth(net, bw)
  # pl = random.randint(0, 20) # pkt loss percentage
  # set_packet_loss(net, pl) 
  # dt = random.randint(0, 10) # delay time 1 - 10ms
  # set_packet_delay(net, dt)
  # layers_removed = random.randint(2, len(net.switches))
  # remove_hops(topo, layers_removed) # remove layers # update active switches
    
if __name__ == "__main__":
  read_probabilities = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2] 
  #test = [0.8, 0.6, 0.4, 0.2]
  simulation_time = 60 # seconds
  # run(read_probabilities, simulation_time)
  plot_from_files(read_probabilities)
