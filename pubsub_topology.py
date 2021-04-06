from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink


class PubSub_Topo(Topo):
    # "Publisher & Subscriber Topology."
    # override the build method. We define the number of racks. If Racks == 1,
    # All the publisher and subscriber nodes are on the same rack. If Racks==2, then main
    # node is on rack while publisher nodes are on second rack but subscriber are back on
    # same switch as main node (sounds silly). If Racks==3 then the main is on one
    # rack, the publisher nodes on 2nd rack and subscriber nodes on the third rack. Number of
    # switches equals the number of racks.

    def build(self, Racks=1, P=1, S=1):
        print("Topology: Racks = ", Racks, ", P = ", P, ", S = ", S)
        self.ps_switches = []
        self.ps_hosts = []
        # Python's range(N) generates 0..N-1
        for r in range(Racks):
            # a switch per rack.
            switch = self.addSwitch('s{}'.format(r + 1))
            print("Added switch", switch)
            self.ps_switches.append(switch)
            if (r > 0):
                # connect the switches
                self.addLink(self.ps_switches[r - 1], self.ps_switches[r], delay='5ms')
                print("Added link between", self.ps_switches[r - 1], " and ", self.ps_switches[r])

        host_index = 1
        switch_index = 1
        # add zookeeper host on rack 1, i.e., switch 1
        host = self.addHost('h{}s{}'.format(host_index, switch_index))
        print("Added zookeeper host", host)
        self.addLink(host, self.ps_switches[switch_index], delay='1ms')  # zero based indexing
        print("Added link between ", host, " and switch ", self.ps_switches[switch_index])
        self.ps_hosts.append(host)

        # Now add 3 broker nodes to next available rack
        for i in range(3):
            host_index += 1
            host = self.addHost('h{}s{}'.format(host_index, switch_index + 1))
            print("Added broker host", host)
            self.addLink(host, self.ps_switches[switch_index], delay='1ms')  # zero based indexing
            print("Added link between ", host, " and switch ", self.ps_switches[switch_index])
            self.ps_hosts.append(host)

        # Now add the P publisher nodes to the next available rack
        switch_index = (switch_index + 1) % Racks
        for h in range(P):
            host_index += 1
            host = self.addHost('h{}s{}'.format(host_index, switch_index + 1))
            print("Added next publisher host", host)
            self.addLink(host, self.ps_switches[switch_index], delay='1ms')
            print("Added link between ", host, " and switch ", self.ps_switches[switch_index])
            self.ps_hosts.append(host)

        # Now add the S subscriber nodes to the next available rack
        switch_index = (switch_index + 1) % Racks
        for h in range(S):
            host_index += 1
            host = self.addHost('h{}s{}'.format(host_index, switch_index + 1))
            print("Added next subscriber host", host)
            self.addLink(host, self.ps_switches[switch_index], delay='1ms')
            print("Added link between ", host, " and switch ", self.ps_switches[switch_index])
            self.ps_hosts.append(host)
