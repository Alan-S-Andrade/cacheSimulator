from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import OVSSwitch, Controller
from mininet.link import TCLink
from mininet.node import Host
from mininet.log import setLogLevel
import time
from xmlrpc.server import SimpleXMLRPCServer
import socket

# define original network topology
class DynamicTopology(Topo):
    def __init__(self):
        super().__init__()
        self.num_switches = 2
        self.num_servers_per_switch = 8
        self.servers = []
        self.switch_names = [num for num in range(3, 11)]
    
    # Function to start a cache server
    def start_cache_server(name, net):
        cache_server = net.addHost(name, cls=Host)
        return cache_server

    # Function to start the central server
    def start_central_server(net):
        central_server = net.addHost('central_server', cls=Host)
        return central_server
    
    def add_some_servers(self, switch, net):
        for i in range(10):
            server = net.addHost(f's3-h{i+1}')
            self.servers.append(server)
            net.addLink(server, switch)
        print(f'10 nodes added to {switch}')
        net.stop()
        net.build()
    
    def add_another_switch(self):
        switch = self.addSwitch('s3')
        return switch

    def add_servers(self, switch):
        for i in range(self.num_servers_per_switch):
            server = self.addHost(f's{switch}-h{i+1}')
            self.servers.append(server)
            self.addLink(server, switch)
            

    def remove_servers(self, switch, net):
        for i in range(self.num_servers_per_switch):
            server = self.servers.pop(0)
            for link in net.links:
                if link.intf1.node == switch and link.intf2.node == server:
                    # Found the link between switch and host
                    net.delLink(link)
                    net.delHost(server)
                    break
        print(f'Nodes removed from {switch}')


    def create_topology(self):
        for i in range(self.num_switches):
            switch = self.addSwitch(f's{i+1}')
            self.add_servers(switch)
    
    def link_switches(self):
        for i in range(self.num_switches - 1):
            self.addLink(f's{i+1}', f's{i+2}')
    
    def set_bandwidth(self, net, bw):
        for link in net.links:
            link.intf1.config(bw=bw)
            link.intf2.config(bw=bw)

    def get_host_ip(self, net, host):
        if isinstance(host, Host):
            for intf_name in host.intfNames():
                intf = host.intfs[intf_name]
                if intf.IP():
                    return intf.IP()
        return None

    # set cache percentages on all clients
    def set_cache_percentages(client, read_percentage, write_percentage):
        # Iterate over all clients
        for client_address in client.clients:
            # Set cache percentages for each client
            client.set(f'{client_address}:read_percentage', read_percentage)
            client.set(f'{client_address}:write_percentage', write_percentage)

    # get cache percentages from all clients
    def get_cache_percentages(client):
        percentages = {}
        # Iterate over all clients
        for client_address in client.clients:
            # Get cache percentages for each client
            read_percentage = client.get(f'{client_address}:read_percentage')
            write_percentage = client.get(f'{client_address}:write_percentage')
            percentages[client_address] = {'read_percentage': read_percentage, 'write_percentage': write_percentage}
        return percentages
    
    # perform RPC call from cache server to central server
    def rpc_call(key):
        with xmlrpc.client.ServerProxy(f"http://{CENTRAL_SERVER_ADDRESS}:{CENTRAL_SERVER_PORT}/") as proxy:
            return proxy.read(key)

def main():
    setLogLevel('info')

    topo = DynamicTopology()
    topo.create_topology()
    topo.link_switches()

    net = Mininet(topo=topo, switch=OVSSwitch, controller=Controller, link=TCLink)

    # Start central server
    # central_server = topo.start_central_server()
    # net.addLink('central_server', 's1')

    # Start cache servers
    net.start()

    try:
        while True:
            # Run ping test
            net.pingAll()

            host_ip = topo.get_host_ip(net, 'h1')
            if host_ip:
                print(f"Host 'h1' IP address: {host_ip}")
                # read_percentage = 70
                # write_percentage = 30
                # topo.adjust_percentages(read_percentage, write_percentage)
                # Store data in Mininet nodes
                #topo.store_data(client, 'key1', 'value1')
                # Send invalidate message
                # invalidate_message = 'INVALIDATE key1'
                # topo.send_invalidate_messages(invalidate_message)

                # # Retrieve data from Mininet nodes
                # # data = topo.retrieve_data(client, 'key1')
                # print("Retrieved data:", data)
            # else:
                # print("Host 'h1' has no IP address.")

            # Remove servers
            time.sleep(1)  # Remove some servers every 10 seconds from switch 1
            topo.remove_servers('s1', net)

            # Dynamically add another switch servers: s3 (rename each time)
            # Then add some servers to that switch
            # Then connect new switch to switch1
            time.sleep(1)  # Add servers every 10 seconds
            newSwitch = net.addSwitch(f's{topo.switch_names.pop()}')
            topo.add_some_servers(newSwitch, net)
            net.addLink(newSwitch, 's1')
            print(f'New Switch {newSwitch} added')

            # Re-add nodes to s1
            time.sleep(10)
            topo.add_some_servers('s1', net)

            # Set bandwidth to 30 Mbps
            topo.set_bandwidth(net, bw=30)

            # Remove nodes from s2
            time.sleep(10)
            topo.remove_servers('s2', net)

            # Dynamically add another switch servers: s4 (rename each time)
            # Then add some servers to that switch
            # Then connect new switch to switch2
            time.sleep(1)  # Add servers every 10 seconds
            newSwitch = net.addSwitch(f's{topo.switch_names.pop()}')
            print(f'New Switch {newSwitch} added')
            topo.add_some_servers(newSwitch, net)
            net.addLink(newSwitch, 's2')

            # Set bandwidth to 10 Mbps
            topo.set_bandwidth(net, bw=10)

    except KeyboardInterrupt:
        pass

    net.stop()

if __name__ == '__main__':
    main()
