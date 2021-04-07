import zk_clientservice as kzcli
import time

brokers_root_path = "/brokers"
topic_root_path = "/topics"

pubs_root_path = "/pubs"
subs_root_path = "/subs"

topic_path = "/topics/t2"

brokers = []
broker_state_map = {}
topics_being_watched = []

zkclient = kzcli.ZkClientService()
brokers = zkclient.get_children(brokers_root_path)

def bootstrap_root_nodes():
    zkclient.create_node(brokers_root_path)
    zkclient.create_node(topic_root_path)
    zkclient.create_node(pubs_root_path)
    zkclient.create_node(subs_root_path)

bootstrap_root_nodes()

def watch_topics_root(event):
    topics = zkclient.get_children(topic_root_path)
    for topic in topics:
        zkclient.watch_individual_node(topic_root_path + "/" + topic, watch_a_topic_change)
    zkclient.watch_node(topic_root_path, watch_topics_root)

def watch_a_topic_change(event):
    print("Broker node changed")
    if event.type == "DELETED":
        print("no topic publisher")
    elif event.type == "CHANGED":
        print("data for the topic has changed")
    topic_change_path = event.path
    zkclient.watch_individual_node(topic_change_path, watch_a_topic_change)


zkclient.watch_node(topic_root_path, watch_topics_root)
topics = zkclient.get_children(topic_root_path)
for topic in topics:
    zkclient.watch_individual_node(topic_root_path + "/" + topic, watch_a_topic_change)
#zkclient.watch_individual_node(topic_path, watch_a_topic_change)


def watch_child_change(event):
    print("Broker node changed")
    if event.type == "DELETED":
        print("no topic publisher")
    elif event.type == "CHANGED":
        print("data for the topic has changed")
    elif event.type == "CHILD":
        print("child node has been added")
    else:
        print(event.Type)
    zkclient.watch_node(topic_root_path, watch_child_change)






while True:
    time.sleep(1000000000)

