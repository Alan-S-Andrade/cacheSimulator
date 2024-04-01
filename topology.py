from mininet.topo import Topo

class DynamicTopology(Topo):
    def __init__(self):
        super().__init__()
        self.num_switch_layers = 2 # Number of switches
        self.hosts_per_switch = 2 # Number of hosts per switch
        self.server_ip = "10.0.0.254" # home server IP
        self.all_switches = []  # store all switches across layers

    def create_network(self):
        # Create home node
        home = self.addHost("home", ip=self.server_ip)

        # Create switch layers and connect them linearly
        prev_switch = None  # Store the switch from the previous layer
        for layer in range(self.num_switch_layers):
            # Create a switch for the current layer
            switch = self.addSwitch(f"s{layer}")
            self.all_switches.append(switch)

            # Connect server to first layer switch
            if layer == 0:
                self.addLink(home, switch)

            # Connect hosts to the switch in the current layer
            for i in range(self.hosts_per_switch):
                host = self.addHost(f"s{layer}_n{i}", ip=f"10.0.{layer}.{i+1}")
                self.addLink(host, switch)

            # Connect switch from the previous layer
            if prev_switch:
                self.addLink(prev_switch, switch)
            prev_switch = switch