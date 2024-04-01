from mininet.net import Mininet
from mininet.net import Mininet
from mininet.node import OVSSwitch
from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.cli import CLI
import time
import random
import multiprocessing as mp
from topology import DynamicTopology

def configure_cache(net):
  for host in net.hosts:
    if host.name != 'nat0':
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

def run_app_on_host(host):
  if host.name != 'home':
    host.cmd("python3 cache_app.py")

def run_cache_app(net):
  processes = []
  read_probabilities = [0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]

  for host in net.hosts:
    if host.name != 'home':
      for read_probability in read_probabilities:
        cmd = f"python3 cache_app.py {read_probability}"
        process = mp.Process(target=host.cmd, args=(cmd,))
        process.start()
        processes.append(process)

  # Wait for all processes to finish
  for process in processes:
      process.join()

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

def remove_hops(topo, layers_removed):
  for switch in range(layers_removed, len(topo.all_switches)):
    print(topo.all_switches[switch])  # Stop the switch processes in the layer

def add_hops(topo, net, layers_removed):
  for switch in range(layers_removed, len(topo.all_switches)):
    topo.all_switches[switch].start()  # Start the switch processes in the layer
  net.waitConnected()

def run():
  setLogLevel("debug")

  topo = DynamicTopology()
  topo.create_network()

  net = Mininet(topo=topo, switch=OVSSwitch, waitConnected=True, link=TCLink)

  # Start the network
  net.start()

  # Install and configure Redis on hosts
  configure_cache(net)

  start_time = time.time()
  # run app on every node 
  while time.time() - start_time < 60:  # Run for one minute
    bw = random.uniform(0.1, 10)  # random bandwidth between 1 and 10 Mbps
    set_bandwidth(net, bw)
    pl = random.randint(0, 20) # pkt loss percentage
    set_packet_loss(net, pl) 
    dt = random.randint(0, 10) # delay time 1 - 10ms
    set_packet_delay(net, dt)
    layers_removed = random.randint(2, len(net.switches))
    remove_hops(topo, layers_removed) # remove layers # update active switches
    
    # run app on all nodes
    run_cache_app(net)
    time.sleep(10) 

    add_hops(topo, net, layers_removed) # re-enalbe switches

  net.stop()

if __name__ == "__main__":
   run()
