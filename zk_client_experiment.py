import sys
import os
import logging as logger

import kazoo.client as cl
import kazoo.exceptions as ke
import host_ip_provider as hip
import constants as const
import zk_clientservice as zkcl
import time

print(sys.argv)

kzclient = zkcl.ZkClientService()

def watch_broker_func(event):
    print("Broker node changed")
    broker_strategy_reconnect_and_publish()

def broker_strategy_reconnect_and_publish():
    active_broker_node_name = kzclient.get_broker_node_name(const.LEADER_ELECTION_ROOT_ZNODE)
    if active_broker_node_name == "":
        print("No broker is running, existing the publisher app!")
        os._exit(0)
    active_broker_node_value = kzclient.get_broker(const.LEADER_ELECTION_ROOT_ZNODE)
    print("Setting watch on leader broker node_path:{}".format(const.LEADER_ELECTION_ROOT_ZNODE + '/' + active_broker_node_name))
    kzclient.watch_individual_node(const.LEADER_ELECTION_ROOT_ZNODE + '/' + active_broker_node_name, watch_broker_func)   
    broker_ip = active_broker_node_value.split(':')[0]
    broker_listening_port = active_broker_node_value.split(':')[1].split(',')[0]
    active_broker_ip_port = broker_ip + ":" + broker_listening_port
    print("Broker leader is:{}".format(active_broker_ip_port))

broker_strategy_reconnect_and_publish()

while True:
    time.sleep(5)

'''
try:
    zk = cl.KazooClient()
    zk.start()

    ehmemeral_node_path = ""
    def try_elect_leader(nodes):
        print(nodes)
        nodes.sort()
        print(nodes)
        if ehmemeral_node_path.endswith(nodes[0]):
            print("I'm the leader")
        else:
            present_broker_node = const.LEADER_ELECTION_ROOT_ZNODE +'/' + nodes[0]
            data, _ = zk.get(present_broker_node)
            print("current broker ip:port = {}".format(data.decode("utf-8")))
            
            this_broker_node_name = "broker_" + ehmemeral_node_path[len(ehmemeral_node_path)-10:]
            this_broker_node_index = nodes.index(this_broker_node_name)
            #watch for the broker with the next lower index e.g. broker "broker_0000000003" will watch "broker_0000000002"
            #broker "broker_0000000002" will watch "broker_0000000001"
            #and broker "broker_0000000001" will not watch anyone, as it's the leader
            node_to_watch_index = this_broker_node_index - 1
            node_being_followed = const.LEADER_ELECTION_ROOT_ZNODE+'/'+ nodes[node_to_watch_index]
            zk.get(node_being_followed, watch=watch_func)  

    def watch_func(event):
        print("node changed:{}".format(event))
        if event.type == "DELETED":
            childrens = zk.get_children(const.LEADER_ELECTION_ROOT_ZNODE)
            try_elect_leader(childrens)
   
    print(hip.get_host_ip().encode('utf-8'))
    ehmemeral_node_path = zk.create(const.LEADER_ELECTION_ROOT_ZNODE + const.BROKER_NODE_PREFIX, hip.get_host_ip().encode('utf-8') , makepath = True, ephemeral=True, sequence=True)
    broker_nodes = zk.get_children(const.LEADER_ELECTION_ROOT_ZNODE)
    #if there're less than 2 broker left in the system, then fault tolerant quorum cannot be formed
    if len(broker_nodes) < 2:
        print("Not enough broker to form a quorum, minimum needed broker = 2")
        sys.exit()

    try_elect_leader(broker_nodes)

    print(ehmemeral_node_path)
    #node = zk.get_children("/my/favorite", watch=my_func)
    zk.get(ehmemeral_node_path, watch=watch_func)

    while True:
        pass

    @zk.ChildrenWatch("/my/favorite")
    def watch_children(children):
         print("Children are now: %s" % children)  

    children = zk.get_children('/')

    @zk.DataWatch("/my/favorite")
    def watch_Data(data, stat):
        print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))

    @zk.ChildrenWatch("/my/favorite")
    def watch_children(children):
         print("Children are now: %s" % children)
    node = zk.get_children("/my/favorite", watch=my_func)   

    if zk.exists("/my/favorite") is None:
        result = zk.create("/my/favorite/node5", b"node_value_5", makepath = True)
   
    if zk.exists("/my/znode/node2") is None:
        result = zk.create("/my/znode/node2", b"node2_value", makepath = True, ephemeral=True, sequence=True)
        print(result)

    #zk.create("/my/favorite/node", b"a value",makepath=True)
    # Determine if a node exists
    if zk.exists("/my/favorite"):
        # Do something

        # Print the version of a node and its data
        if zk.exists("/my/favorite/node"):
            zk.set("/my/favorite/node", b"some data")

        data, stat = zk.get("/my/favorite/node")
        print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))

        

        # List the children
        children = zk.get_children("/my/favorite")
        print("There are %s children with names %s" % (len(children), children))

    print(children)
  
except ke.KazooException as e:
    logger.error('Kazoo exception: '+ str(e))
    print(e)
finally:
    zk.stop()
    print("zookeeper client stopped")
    '''