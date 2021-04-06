import zmq
import time, threading
import sys
import zk_leaderelector as le
import constants as const
import zk_clientservice as kzcl

'''
The functions of the broker:
 Command line params: i) zookeeper_ip:port ii) broker_port e.g. python3 lamebroker.py 5000
 1. Creates an ephemeral sequence node in the zookeeper under /leaderelection using LeaderElector object
    e.g. /leaderelection/broker_0000000001, node_value = "brokerip:port"
 2. Try to elect leader with LeaderElector, and keep watch on the other stand-by broker nodes 
    by registering a callback function with LeaderElector object, which is called when a broker leader node changes.
    And as part of this callback refreshes the topic publishers list as well
 3. Also, every watch for the publishers nodes change under /pubs
 4. Bind to a tcp socket on all network interfaces on the given port, by default it binds to port 7000
    for the subscribers to connect to retrieve publisher's topics and ip:port
'''
# args - python3 lamebroker.py 9000
topic_publishers = {}

# lock object for syncronizing access to topic_publishers dictionary
lock = threading.Lock()

port = "7000"
if len(sys.argv) > 1:
    port = sys.argv[1]

print('Lame broker started on port:{}'.format(port))


# message parameter format= pub_ip:port#topic1, topic2, topic3 etc.
# The parameter for the call from the subscriber to get topic publishers will be just the name of the topic e.g. "topic1"
def message_processor(message):
    msg_parts = message.split('#')
    # Register publishers ip:port and topics (this data comes from publishers)
    try:
        lock.acquire()
        if len(msg_parts) > 1:
            publisher_ip_port = msg_parts[0]
            topics = msg_parts[1].split(',')
            for topic in topics:
                if topic in topic_publishers:
                    topic_publisher = topic_publishers[topic]
                    topic_publishers[topic] = topic_publisher + ',' + publisher_ip_port
                else:
                    topic_publishers[topic] = publisher_ip_port
            return "publisher registered"
        # get the publishers for the given topic (this is call from subscriber)
        elif len(msg_parts) == 1:
            topic = msg_parts[0]
            if topic in topic_publishers:
                return topic_publishers[topic]
    finally:
        lock.release()
    return ""


zk_client_svc = kzcl.ZkClientService()


def publishers_change_watch_func(event):
    refresh_publishers()


def refresh_publishers():
    print("Trying to refresh the publishers")
    publishers_nodes = zk_client_svc.get_children(const.PUBLISHERS_ROOT_PATH)
    topic_publishers.clear()
    if publishers_nodes != None and len(publishers_nodes) > 0:
        for pub_node in publishers_nodes:
            pub_node_data = zk_client_svc.get_node_value(const.PUBLISHERS_ROOT_PATH + '/' + pub_node)
            message_processor(pub_node_data)
        print("List of topic publishers are: {}".format(topic_publishers))
    else:
        print("There're no topic publishers available!")
    zk_client_svc.watch_node(const.PUBLISHERS_ROOT_PATH, publishers_change_watch_func)


def leader_election_callback(leader_broker_ip_port):
    print("The current leader is: {}".format(leader_broker_ip_port))
    # hydrate publishers and topic
    refresh_publishers()


# Try to elect a broker leader
# The broker node value for direct strategy e.g. node_path = /leaderelection/broker_0000000001, node_value = broker_ip:listening_port
# e.g node_value = 10.0.0.5:2000
leader_elector = le.LeaderElector(zk_client_svc, const.LEADER_ELECTION_ROOT_ZNODE, const.BROKER_NODE_PREFIX)
leader_elector.try_elect_leader(leader_election_callback, port)

# Bind the tcp socket to the supplied port
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:%s" % port)

while True:
    # Wait for next request from client
    message = socket.recv_string()
    print("Received request: {}".format(message))
    response = message_processor(message)
    print("Sending response: {}".format(response))
    socket.send_string(response)
