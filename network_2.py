from mininet.net import Mininet
from mininet.node import Host, OVSSwitch
from mininet.topo import Topo
from mininet.clean import cleanup
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import OVSSwitch, OVSController, Controller
from mininet.link import TCLink
from mininet.log import setLogLevel
import time
from xmlrpc.server import SimpleXMLRPCServer
import socket
from mininet.cli import CLI
    
# adds new layer with switch and servers connected to it
# prev_switch is the switch above the new layer
def add_layer(net, layer_num, switch_prev):
    switch_curr = net.addSwitch('s{}'.format(layer_num), stp=True)
    # Reassign the controller for the new switch
    net.addLink(switch_curr, net.controllers[0])
    for controller in net.controllers:
        controller.start()
    switch_curr.start([net.controllers[0]])

    net.ping([switch_curr, net.controllers[0]])

    # Add 8 server nodes connected to the current switch
    # for i in range(1, 9):
    #     server = net.addHost(f's{layer_num}_h{i}', ip=f'10.0.{layer_num}.0.{i+1}')
    #     net.addLink(server, switch_curr)

    # Connect the current switch to the previous switch
    net.addLink(switch_curr, switch_prev)
    net.waitConnected()
    
    return switch_curr


def main():
    setLogLevel('debug')

    topo = Dyna
    net = Mininet(topo=topo, switch=OVSSwitch, link=TCLink, controller=OVSController, waitConnected=True)

    # Start the network
    net.addNAT().configDefault()
    net.start()
    for controller in net.controllers:
        controller.start()
    # sss = net.addSwitch('sss')
    # sss.checkSetup()
    # sss.configDefault()

    # Test connectivity
    #net.pingAll()

    # Add 2 additional layers of switches and server nodes
    # switch_prev = net.get('s1')
    # for layer_num in range(2, 3):  
    #     switch_prev = add_layer(net, layer_num, switch_prev)
    
    # Test connectivity after adding more layers
    # hosts = [net.getNodeByName('manager'), net.getNodeByName('s2_h2'), net.getNodeByName('s1_h3')]
    # print(net.ping(hosts=hosts))
    # # net.pingAll()

      # Install and run a simple web server on the host (using httpd package)
    print("Installing and running web server on s1h1...")
    h1 = net.get('s1_h1')
    h1.cmd('echo "nameserver 8.8.8.8" > /etc/resolv.conf')
    # h1.cmd('sudo apt-get update && sudo apt-get install -y apache2')
    h1.cmd('sudo atp-get install vim')
    h1.cmd('service apache2 start')
    h1.cmd('echo "<h1>Welcome to Mininet Apache Server</h1>" > /var/www/html/index.html')

    # Print the web server's IP address
    while True:
        print(f"Web server running on h1 with IP: {h1.IP()}")
        h2 = net.get('s1_h2')
        h1.cmd('iptables -F')
        h1.cmd('curl http://localhost:80')
        h1.cmd('ps aux | grep apache2')
        h1.cmd('netstat -atlpn | grep :80')
        h1.cmd('cat /etc/apache2/apache2.conf | grep Listen')
        # h1.cmd('vim /etc/apache2/apache2.conf')
        # ?h2.cmd('apt-get update && apt-get install -y links')
        # h2.cmd('links http:10.0.0.2:80')
        CLI(net)
        time.sleep(100)

    # Stop the network
    net.stop()

if __name__ == '__main__':
    main()
