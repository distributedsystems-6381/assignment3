import sys
import csv
from datetime import datetime
import threading

import host_ip_provider as hip
import direct_sub_middleware as dmw
import broker_sub_middleware as bmw
import zmq
import zk_clientservice as kzcl
import constants as const
import multiprocessing as mp
import time
import os
from pathlib import Path

'''
 args python3 {direct or broker} {zookeeper_ip:port} topic1 topic2
 1. Get current active broker_ip:port from zookeeper
k 2. Retrieve publishers for the topics of the interest
 3. Watch for the active broker node in zookeeper
'''
# e.g args "python3 subscriber_app.py direct 127.0.0.1:2181 topic1 topic2"
# capture subscriber IP for use in logger_function
subscriber_ip = hip.get_host_ip()
publishers = []
# The process to run the subscribers
process_list = []
# Extract the strategy to discover and disseminate the messages
strategy = ""
if len(sys.argv) > 1:
    strategy = sys.argv[1]

if strategy != "direct" and strategy != "broker":
    print("Please submit valid strategy (direct || broker)")
    sys.exit()

# Get zookeeper ip and port, passed in arg[2] as ip:port e.g. 127.0.0.1:2181
zookeeper_ip_port = ""
if len(sys.argv) > 2:
    zookeeper_ip_port = sys.argv[2]

if zookeeper_ip_port == "":
    print("No zookeeper ip:port provided, exiting subscriber application.")
    sys.exit()

# Add topics of interests
subscribed_topics = []
if len(sys.argv) > 3:
    for arg in sys.argv[3:]:
        subscribed_topics.append(arg)
    print("Topics to subscribe:{}".format(subscribed_topics))

if len(subscribed_topics) == 0:
    print("Please provide topics to subscribe)")
    sys.exit()


def logger_function(message):
    topic_data, message_id, message_sent_at_timestamp, publisher_ip = message.split("#")
    datetime_sent_at = datetime.strptime(message_sent_at_timestamp, '%Y-%m-%dT%H::%M::%S.%f')
    date_diff = datetime.now() - datetime_sent_at
    total_time_taken_milliseconds = date_diff.total_seconds() * 1000
    print('topic_data: {},'
          'message_id: {},'
          'message_sent_at_timestamp: {},'
          'publisher_ip: {}'.format(topic_data, message_id, datetime_sent_at, publisher_ip))

    output_folder = Path("output/")
    output_file = output_folder / "topic_meta_data.out"
    with open(output_file, mode='a') as topic_meta_data_file:
        topic_meta_data_writer = csv.writer(topic_meta_data_file, delimiter=',', quotechar='"',
                                            quoting=csv.QUOTE_MINIMAL)
        topic_meta_data_writer.writerow([publisher_ip, subscriber_ip, message_id, total_time_taken_milliseconds])


def notify(topic, message):
    print("Data received by this app, topic: {}, message: {}".format(topic, message))
    logger_thread = threading.Thread(target=logger_function, args=(message,), daemon=True)
    logger_thread.start()


# direct implementation
def direct_messaging_strategy(pubs, topics):
    # create the SubscriberMiddleware and register the topics of interest and the notify callback function
    publisher = dmw.DirectSubMiddleware(pubs)
    publisher.register(topics, notify)


def broker_messaging_strategy(brokers, topics):
    # create the BrokerSubscriberMiddleware and register the topics of interest and the notify callback function
    broker = bmw.BrokerSubMiddleware(brokers)
    broker.register(topics, notify)


# initiate messaging based on which strategy is submitted
def start_receiving_messages(subscribing_strategy, topics_publishers):
    if subscribing_strategy == "direct" and topics_publishers is not None:
        direct_messaging_strategy(topics_publishers, subscribed_topics)
    elif subscribing_strategy == "broker" and topics_publishers is not None:
        broker_messaging_strategy(topics_publishers, subscribed_topics)
    else:
        print("Check that all necessary values have been submitted")


# Watch function for the broker node change
def watch_broker_func(event):
    print("Broker node changed")
    # If the broker changes terminate the previous process
    # and get the topic publisher from the new broker and
    # start receving the topic messages
    if len(process_list) > 0:
        thr = process_list[0]
        thr.terminate()
        process_list.pop(0)

    if strategy == "direct":
        get_publishers(broker_ip_port)
    elif strategy == "broker":
        broker_strategy_reconnect_and_receive()


kzclient = kzcl.ZkClientService()
broker_ip_port = ""


# get the current active broker ip:port from the zookeeper
def get_publishers(broker_ip_port):
    # The broker node value for direct strategy e.g. node_path = /leaderelection/broker_0000000001, node_value = broker_ip:listening_port
    # e.g node_value = 10.0.0.5:2000
    active_broker_node_name = kzclient.get_broker_node_name(const.LEADER_ELECTION_ROOT_ZNODE)
    if active_broker_node_name == "":
        print("No broker is running, existing the subscriber app!")
        os._exit(0)
        return

    active_broker_ip_port = kzclient.get_broker(const.LEADER_ELECTION_ROOT_ZNODE)
    if active_broker_ip_port == broker_ip_port:
        print("There is no change in active broker")
        return

    # Retrieve message publishers from the active broker
    print("Retrieving topic publishers from active broker running at ip:port => {}".format(zookeeper_ip_port))
    context = zmq.Context()
    broker_socket = context.socket(zmq.REQ)
    broker_socket.connect("tcp://{}".format(active_broker_ip_port))
    for topic in subscribed_topics:
        broker_socket.send_string(topic)
        message = broker_socket.recv_string()
        print("Message received from broker: {}".format(message))
        if message == '':
            print("There are no publishers for the topic: {}".format(topic))
            continue
        topic_publishers = message.split(',')
        for topic_publisher in topic_publishers:
            publishers.append(topic_publisher)
    kzclient.watch_node(const.LEADER_ELECTION_ROOT_ZNODE + '/' + active_broker_node_name, watch_broker_func)
    if len(publishers) != 0:
        print("Publishers for the topics:{}".format(publishers))
        thr = mp.Process(target=start_receiving_messages, args=(strategy, publishers))
        process_list.append(thr)
        thr.start()
        # start_receiving_messages(strategy, publishers)
    else:
        print("There are no publisers for these topics:{}".format(subscribed_topics))


def broker_strategy_reconnect_and_receive():
    brokers = []
    # Get active broker_ip_port
    active_broker_node_name = kzclient.get_broker_node_name(const.LEADER_ELECTION_ROOT_ZNODE)
    if active_broker_node_name == "":
        print("No broker is running, existing the subscriber app!")
        os._exit(0)
        return
    active_broker_node_value = kzclient.get_broker(const.LEADER_ELECTION_ROOT_ZNODE)
    # For broker strategy, the broker node_value is in this format, node_value  = broker_ip:listening_port,publishing_port
    # e.g 10.0.0.5:2000,3000 
    broker_ip = active_broker_node_value.split(':')[0]
    broker_publishing_port = active_broker_node_value.split(':')[1].split(',')[1]
    active_broker_ip_port = broker_ip + ":" + broker_publishing_port
    print("Broker leader is publishing message at ip_port:{}".format(active_broker_ip_port))
    brokers.append(active_broker_ip_port)
    kzclient.watch_node(const.LEADER_ELECTION_ROOT_ZNODE + '/' + active_broker_node_name, watch_broker_func)
    thr = mp.Process(target=start_receiving_messages, args=(strategy, brokers))
    process_list.append(thr)
    thr.start()
    # start_receiving_messages(strategy, brokers)


# Start the message pump based upon messaging strategy
if strategy == "direct":
    get_publishers(broker_ip_port)
elif strategy == "broker":
    broker_strategy_reconnect_and_receive()

while True:
    time.sleep(10)
