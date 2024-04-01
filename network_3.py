import time
from mininet.net import Mininet
from mininet.cli import CLI
from mininet.log import setLogLevel

# Set Mininet logging level (optional)
setLogLevel('debug')  # Adjust for desired logging level (e.g., 'debug', 'warning')

def run_web_server():
  """
  Creates a simple Mininet network and runs a web server on a host.
  """
  # Create a Mininet instance
  net = Mininet()

  # Add a controller
  net.addController('c0')

  # Add a switch and a host
  s1 = net.addSwitch('s1')
  h1 = net.addHost('h1')
  net.addLink(s1, h1)

  # Start the network
  net.start()

  # Install and run a simple web server on the host (using httpd package)
  print("Installing and running web server on h1...")
  h1.cmd('ping 8.8.8.8')

  h1.cmd('echo "nameserver 8.8.8.8" > /etc/resolv.conf')
  h1.cmd('sudo apt-get update && sudo apt-get install -y apache2')
  h1.cmd('service apache2 start')

  # Print the web server's IP address
  print(f"Web server running on h1 with IP: {h1.IP()}")

  while True:
    time.sleep(100)
    print(h1.IP())

  # Optional: Use the Mininet CLI for further interaction
  # CLI(net)

  # Stop the network
  net.stop()

if __name__ == '__main__':
  run_web_server()
