#!/usr/bin/env python3

import os  # OS level utilities
import sys
import argparse  # for command line parsing
import shutil

# These are all Mininet-specific
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.net import CLI
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel, info
from mininet.util import pmonitor

# This is our topology class created specially for Mininet
from pubsub_topology import PubSub_Topo

from mininet.node import OVSController


##################################
# Command line parsing
##################################
def parseCmdLineArgs():
    # parse the command line
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, default="127.0.0.1", help="Zookeeper IP, default 127.0.0.1")
    parser.add_argument("-p", "--port", type=int, default=2181, help="Zookeeper port, default is 2181")
    parser.add_argument("-r", "--racks", type=int, choices=[1, 2, 3, 4], default=1,
                        help="Number of racks, choices 1, 2, 3, or 4")
    parser.add_argument("-t", "--strategy", type=str, choices=["direct", "broker"], default="direct",
                        help="Dissemination strategy to use: direct or broker, default is direct")
    parser.add_argument("-P", "--publishers", type=int, default=1, help="Number of publishers, default 1")
    parser.add_argument("-PT", "--publishertopics", required=True, type=str, nargs='*', default="",
                        help="topics to publish")
    parser.add_argument("-S", "--subscribers", type=int, default=1, help="Number of subscribers, default 1")
    parser.add_argument("-ST", "--subscribertopics", required=True, type=str, nargs='*', default="",
                        help="topics to subscribe to")

    # parse the args
    args = parser.parse_args()
    return args


def convertToStr(input_seq):
    # Join all the strings in list
    final_str = ' '.join(input_seq)
    return final_str


def runCommands(net, args):
    try:
        src_dir_path = "/home/tito/workspace/assignment2"

        # create zookeeper command
        host = 0
        zk_path = "/home/tito/workspace/zookeeper"  # change directory for zookeeper
        zk_cmd = "sudo " + zk_path + "/bin/zkServer.sh start &"
        print("host #" + str(net.hosts[host]))
        print("command to send: " + zk_cmd + "\n")
        net.hosts[host].sendCmd(f'sudo {zk_path}/bin/zkServer.sh start &')
        host += 1

        # broker commands
        if str(args.strategy) == "direct":
            lamebroker_cmd = f'python3  {src_dir_path}/lamebroker.py 5000 &'
            for i in range(3):
                print(f'host # {net.hosts[host]}')
                print(f'command to send: "{lamebroker_cmd}\n')
                net.hosts[host].sendCmd(f'python3 {src_dir_path}/lamebroker.py 5000 &')
                host += 1
        else:
            broker_cmd = f'python3 {src_dir_path}broker.py 5559 5560 &'
            for i in range(3):
                print("host #" + str(net.hosts[host]))
                print(f'command to send: {broker_cmd}\n')
                net.hosts[host].sendCmd(f'python3 {src_dir_path}/broker.py 5559 5560 &')
                host += 1

        # publisher commands
        if str(args.strategy) == "direct":
            for i in range(args.publishers):
                # extract subscriber topics
                pub_tops = convertToStr(args.publishertopics)
                cmd_str = f'python3 {src_dir_path}/publisher_app.py {args.strategy} \"{args.ip}:{args.port}\" 4000 {pub_tops} &'
                print("host #" + str(net.hosts[host]))
                print(f'command to send: {cmd_str}\n')
                net.hosts[host].sendCmd(
                    f'python3 {src_dir_path}/publisher_app.py {args.strategy} \"{args.ip}:{args.port}\" 4000 {pub_tops} &')
                host += 1
        else:
            for i in range(args.publishers):
                # extract subscriber topics
                pub_tops = convertToStr(args.publishertopics)
                cmd_str = f'python3 {src_dir_path}/publisher_app.py {args.strategy} \"{args.ip}:{args.port}\" {pub_tops} &'
                print("host #" + str(net.hosts[host]))
                print(f'command to send: {cmd_str}\n')
                net.hosts[host].sendCmd(
                    f'python3 {src_dir_path}/publisher_app.py {args.strategy} \"{args.ip}:{args.port}\" {pub_tops} &')
                host += 1

        #  next create the commands for subscribers
        for i in range(args.subscribers):
            # extract subscriber topics
            sub_tops = convertToStr(args.subscribertopics)
            # write command
            cmd_str = f'python3 {src_dir_path}/subscriber_app.py {args.strategy} \"{args.ip}:{args.port}\" {sub_tops} &'
            print("host #" + str(net.hosts[host]))
            print(f'command to send: {cmd_str}\n')
            net.hosts[host].sendCmd(
                f'python3 {src_dir_path}/subscriber_app.py {args.strategy} \"{args.ip}:{args.port}\" {sub_tops} &')
            host += 1

    except:
        print("Unexpected error in run mininet:", sys.exc_info()[0])
        raise


######################
# main program
######################
def main():
    # Create and run the publishers & subscribers via direct or broker strategy in Mininet topology

    # parse the command line
    parsed_args = parseCmdLineArgs()

    # instantiate our topology
    print("Instantiate topology")
    topo = PubSub_Topo(Racks=parsed_args.racks, P=parsed_args.publishers, S=parsed_args.subscribers)

    # create the network
    print("Instantiate network")
    net = Mininet(topo, link=TCLink, controller=OVSController)

    # activate the network
    print("Activate network")
    net.start()

    # debugging purposes
    print("Dumping host connections")
    dumpNodeConnections(net.hosts)

    # debugging purposes
    print("Testing network connectivity")
    net.pingAll()

    # clean up data from previous zookeeper deploys
    dir_path = "/tmp/zookeeper"
    try:
        shutil.rmtree(dir_path)
        print("Directory '%s' has been removed successfully" % dir_path)
    except OSError as e:
        print("Error: %s : %s" % (dir_path, e.strerror))

    print("Running deploy commands\n")
    runCommands(net, parsed_args)

    # run the cli
    CLI(net)

    # cleanup
    net.stop()


if __name__ == '__main__':
    # Tell mininet to print useful information
    setLogLevel('info')
    main()
