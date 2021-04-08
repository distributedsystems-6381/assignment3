import zk_clientservice as kzcli
import time

brokers_root_path = "/brokers"
topic_root_path = "/topics"

pubs_root_path = "/pubs"
subs_root_path = "/subs"

pubs_broker_assignment_root = "/pubsbroker"
subs_broker_assignment_root = "/subsbroker"
brokers_ip_port_root_path = "/brokers_ipport"

brokers = []
broker_state_map = {}
topics_being_watched = []

zkclient = kzcli.ZkClientService()
brokers = zkclient.get_children(brokers_root_path)
if brokers is not None and len(brokers) > 0:
    brokers.sort()

publishers_topics_map = {}
subscribers_topics_map = {}

def bootstrap_root_nodes():
    zkclient.create_node(brokers_root_path)
    zkclient.create_node(topic_root_path)
    zkclient.create_node(pubs_root_path)
    zkclient.create_node(subs_root_path)

bootstrap_root_nodes()

def watch_brokers_root(event): 
    global brokers   

    brokers = zkclient.get_children(brokers_root_path)
    brokers.sort()
    #If no broker is in active state, make one active
    activate_broker(brokers)
    zkclient.watch_node(brokers_root_path, watch_brokers_root)

#Brokers - watch for brokers joining or leaving the quorum
zkclient.watch_node(brokers_root_path, watch_brokers_root)

#broker activation
def activate_broker(brokers):
    if brokers is None:
        return    
    target_brokers_count = get_target_brokers()
    load_balance_brokers(target_brokers_count)
    
def load_balance_brokers(target_brokers_count):
    #get number of active brokers
    activate_broker_count = get_active_broker_count()

    need_broker_reassignment = False
    # Keep at least one broker active in the system even if there're no topics being published
    if len(brokers) > 0 and target_brokers_count == 0:
        for broker in brokers:
            broker_state = zkclient.get_node_value(brokers_root_path + "/" + broker)
            if broker_state == "1":
                return
        need_broker_reassignment = True
        update_broker_state(brokers[0], "1")

    #if target broker count == active broker count, then no load balancing required
    if target_brokers_count > 0 and activate_broker_count == target_brokers_count:
        return
    
    #if target broker count >= number of brokers, then make all inactive broker active  
    if target_brokers_count >= len(brokers):
        for broker in brokers:
            broker_state = zkclient.get_node_value(brokers_root_path + "/" + broker)
            if broker_state == "0":
                need_broker_reassignment = True
                update_broker_state(broker, "1")
    
    #if the target broker count < number of brokers in the system, then deactivate some of the brokers
    elif target_brokers_count > 0 and target_brokers_count < len(brokers):
        number_of_brokers_to_deactivate = len(brokers) - target_brokers_count
        counter = len(brokers) - 1
        while(counter >= 0 and number_of_brokers_to_deactivate > 0):
            broker_fromend = brokers[counter]
            broker_state = zkclient.get_node_value(brokers_root_path + "/" + broker_fromend)
            if broker_state == "1":
                need_broker_reassignment = True
                update_broker_state(broker_fromend, "0")
                number_of_brokers_to_deactivate = number_of_brokers_to_deactivate - 1
            counter = counter - 1    

    if need_broker_reassignment:
        do_pubs_broker_assignment()
        do_subs_broker_assignment()

def do_pubs_broker_assignment():
    publishers = zkclient.get_children(pubs_root_path)

    if publishers is None or len(publishers) == 0:
        return
    publishers.sort()
    active_brokers = get_active_brokers()

    if active_brokers is None or len(active_brokers) == 0:
        return
    active_brokers.sort()

    active_broker_endIndex = len(active_brokers) - 1
    publisher_end_Index = len(publishers) - 1
    while publisher_end_Index >= 0:
        pub_name = publishers[publisher_end_Index]
        if active_broker_endIndex < 0:
            active_broker_endIndex = len(active_brokers) - 1
        
        active_broker_name = active_brokers[active_broker_endIndex]
        activate_broker_node_value = zkclient.get_node_value(brokers_ip_port_root_path + '/' + active_broker_name)     

        zkclient.set_node_value(pubs_broker_assignment_root + '/' + pub_name,  activate_broker_node_value)

        publisher_end_Index = publisher_end_Index - 1
        active_broker_endIndex = active_broker_endIndex - 1

def get_active_broker_count():    
    return len(get_active_brokers()) 

def get_active_brokers():
    active_brokers = []
    brokers = zkclient.get_children(brokers_root_path)
    if brokers is None:
        return active_brokers

    for broker in brokers:
        broker_state = zkclient.get_node_value(brokers_root_path + "/" + broker)
        if broker_state == "1":
            active_brokers.append(broker)
    return active_brokers

#Update broker state to either "0" or "1"
def update_broker_state(broker, state):
    zkclient.set_node_value(brokers_root_path + "/" + broker, state)
    broker_state_map[broker] = state

def watch_topics_root(event):
    topics = zkclient.get_children(topic_root_path)
    for topic in topics:
        zkclient.watch_individual_node(topic_root_path + "/" + topic, watch_a_topic_change)
    zkclient.watch_node(topic_root_path, watch_topics_root)

#topics - watch topics root for new topics being added removed
zkclient.watch_node(topic_root_path, watch_topics_root)

def watch_a_topic_change(event):
    topic_path_to_watch = event.path
    if event.type == "DELETED":
        print("The topic {} has been deleted".format(topic_path_to_watch))
        return    
    print("topic data has changed")
    target_brokers_count = get_target_brokers_for_a_topic(topic_path_to_watch.split('/')[2])
    load_balance_brokers(target_brokers_count)

    #get topic publishers and subscribers
    zkclient.watch_individual_node(topic_path_to_watch, watch_a_topic_change)

def get_target_brokers():
    topics = zkclient.get_children(topic_root_path)
    target_brokers = [] 
    target_broker_count = 0   
    for topic in topics:
        brokers_needed_for_a_topic = get_target_brokers_for_a_topic(topic)
        if brokers_needed_for_a_topic > 1:
            target_broker_count = target_broker_count + brokers_needed_for_a_topic
        target_brokers.append(target_broker_count)

    return target_broker_count

def get_target_brokers_for_a_topic(toipc_name):
    topic_node_value = zkclient.get_node_value(topic_root_path + "/" + toipc_name)
    target_broker_count = 1
    if topic_node_value is not None:
        topic_pubs_subs = topic_node_value.split('#')
        topic_pubs = []
        topic_subs = []
        if len(topic_pubs_subs) > 0:
            topic_pubs = topic_pubs_subs[0].split(',')

        if len(topic_pubs_subs) > 1:
            topic_subs = topic_pubs_subs[1].split(',')
        
        if len(topic_pubs) > 1 and len(topic_subs) > 1:
            target_broker_count = target_broker_count + 1
    return target_broker_count

   
#topic - start to watch existing individual topics when the load balancer starts
topics = zkclient.get_children(topic_root_path)
if topics is not None:
    for topic in topics:
        zkclient.watch_individual_node(topic_root_path + "/" + topic, watch_a_topic_change)

def update_publishers_topics_maps(pubs):
    if pubs is None or len(pubs) == 0:
        return
    for pub in pubs:
        pub_topics = zkclient.get_node_value(pubs_root_path + "/" + pub)          
        publishers_topics_map[pub] = pub_topics
        

def watch_individual_pub_node(event):
    global publishers_topics_map
    pub_node_path = event.path
    pub_node_name = pub_node_path.split('/')[len(pub_node_path.split('/')) - 1]
    if event.type == "DELETED":
        if pub_node_name in publishers_topics_map:
            published_topics = publishers_topics_map[pub_node_name].split(',')
            for topic in published_topics:
                topic_node_value = zkclient.get_node_value(topic_root_path + "/" + topic)
                if topic_node_value is not None:
                    topic_pubs_subs = topic_node_value.split('#')
                    topic_pubs = []
                    topic_subs = []
                    if len(topic_pubs_subs) > 1:
                        topic_pubs = topic_pubs_subs[0].split(',')
                        topic_subs = topic_pubs_subs[1].split(',')
                    elif len(topic_pubs_subs) == 1:
                        topic_pubs = topic_pubs_subs[0].split(',')

                    if topic_pubs.count(pub_node_name) > 0:
                        topic_pubs.remove(pub_node_name)

                    if topic_pubs.count("") > 0:
                        topic_pubs.remove("")
            
                    if topic_subs.count("") > 0:
                        topic_subs.remove("")

                    if len(topic_pubs) > 0 and len(topic_subs) > 0:
                        zkclient.set_node_value(topic_root_path + "/" + topic, ",".join(topic_pubs) + "#" + ",".join(topic_subs))
                    elif len(topic_pubs) == 0 and len(topic_subs) > 0:
                        zkclient.set_node_value(topic_root_path + "/" + topic, "#" + ",".join(topic_subs))
                    elif len(topic_pubs) == 0 and topic_subs[0] == 0:   
                        zkclient.delete_node(topic_root_path + "/" + topic)

            publishers_topics_map.pop(pub_node_name) 
    else:
        publishers_topics_map[pub_node_name] = zkclient.get_node_value(pub_node_path)
        zkclient.watch_individual_node(pubs_root_path + "/" + pub_node_name, watch_individual_pub_node)


def watch_pubs_root(event):
    all_pubs = zkclient.get_children(pubs_root_path)
    update_publishers_topics_maps(all_pubs)
    if all_pubs is not None:
        for pub in all_pubs:
            zkclient.watch_individual_node(pubs_root_path + "/" + pub, watch_individual_pub_node)
    do_pubs_broker_assignment()
    zkclient.watch_node(pubs_root_path, watch_pubs_root)

#setup watch on pubs root
zkclient.watch_node(pubs_root_path, watch_pubs_root)


#Subscriber watch
def update_sublishers_topics_maps(subs):
    if subs is None or len(subs) == 0:
        return
    for sub in subs:
        sub_topics = zkclient.get_node_value(subs_root_path + "/" + sub)          
        subscribers_topics_map[sub] = sub_topics

def watch_subs_root(event):
    all_subs = zkclient.get_children(subs_root_path)
    update_sublishers_topics_maps(all_subs)
    if all_subs is not None:
        for sub in all_subs:
            zkclient.watch_individual_node(subs_root_path + "/" + sub, watch_individual_sub_node)
    do_subs_broker_assignment()
    zkclient.watch_node(subs_root_path, watch_subs_root)

def do_subs_broker_assignment():
    subscribers = zkclient.get_children(subs_root_path)

    if subscribers is None or len(subscribers) == 0:
        return
    subscribers.sort()
    active_brokers = get_active_brokers()

    if active_brokers is None or len(active_brokers) == 0:
        return
    active_brokers.sort()

    active_broker_endIndex = len(active_brokers) - 1
    subscriber_end_Index = len(subscribers) - 1
    while subscriber_end_Index >= 0:
        sub_name = subscribers[subscriber_end_Index]
        if active_broker_endIndex < 0:
            active_broker_endIndex = len(active_brokers) - 1
        
        active_broker_name = active_brokers[active_broker_endIndex]
        activate_broker_node_value = zkclient.get_node_value(brokers_ip_port_root_path + '/' + active_broker_name)     

        zkclient.set_node_value(subs_broker_assignment_root + '/' + sub_name,  activate_broker_node_value)

        subscriber_end_Index = subscriber_end_Index - 1
        active_broker_endIndex = active_broker_endIndex - 1

def watch_individual_sub_node(event):
    global subscribers_topics_map
    sub_node_path = event.path
    sub_node_name = sub_node_path.split('/')[len(sub_node_path.split('/')) - 1]
    if event.type == "DELETED":
        if sub_node_name in subscribers_topics_map:
            subscribed_topics = subscribers_topics_map[sub_node_name].split(',')
            for topic in subscribed_topics:
                topic_node_value = zkclient.get_node_value(topic_root_path + "/" + topic)
                if topic_node_value is not None:
                    topic_pubs_subs = topic_node_value.split('#')
                    topic_pubs = topic_pubs_subs[0].split(',')
                    topic_subs = []
                    if len(topic_pubs_subs) > 1:
                       topic_subs = topic_pubs_subs[1].split(',')

                    if topic_subs.count(sub_node_name) > 0:
                        topic_subs.remove(sub_node_name)

                    if topic_pubs.count("") > 0:
                        topic_pubs.remove("")
            
                    if topic_subs.count("") > 0:
                        topic_subs.remove("")

                    if len(topic_subs) > 0 and len(topic_pubs) > 0:
                        zkclient.set_node_value(topic_root_path + "/" + topic, ",".join(topic_pubs) + "#" + ",".join(topic_subs))
                    elif len(topic_pubs) == 0 and len(topic_subs) > 0:
                        zkclient.set_node_value(topic_root_path + "/" + topic, "#" + ",".join(topic_subs))
                    elif len(topic_subs) == 0 and len(topic_pubs) > 0:
                        zkclient.set_node_value(topic_root_path + "/" + topic, ",".join(topic_pubs))
                    elif len(topic_pubs) == 0 and len(topic_subs) == 0:
                        zkclient.delete_node(topic_root_path + "/" + topic)
            subscribers_topics_map.pop(sub_node_name) 
    else:
        subscribers_topics_map[sub_node_name] = zkclient.get_node_value(sub_node_path)
        zkclient.watch_individual_node(subs_root_path + "/" + sub_node_name, watch_individual_pub_node)

#setup watch on pubs root
zkclient.watch_node(subs_root_path, watch_subs_root)

#Block the thread to receive the node callbacks
while True:
    time.sleep(1000000000)

