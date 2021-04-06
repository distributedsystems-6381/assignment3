import host_ip_provider as hip
import constants as const
import os

'''
    Leader election algorithm:
    1. Create an emphemeral node
    2. Try to elect leader, by choosing a node with the lowest sequence number
    3. If not the leader, then set watch on the node with next lower index

    e.g. Out of these 3 nodes /leaderelection/broker_0000000001, /leaderelection/broker_0000000002, /leaderelection/broker_0000000003
        /leaderelection/broker_0000000001 = leader
        /leaderelection/broker_0000000002 watches for deletion of /leaderelection/broker_0000000001
        /leaderelection/broker_0000000003 watches for deletion of /leaderelection/broker_0000000002
'''


class LeaderElector():
    def __init__(self, zk_client_svc, leader_election_znode_root_path, leader_node_name_prefix):
        self.kzclient = zk_client_svc
        self.leader_election_znode_root_path = leader_election_znode_root_path
        self.leader_node_name_prefix = leader_node_name_prefix
        self.leader_election_callback = None
        self.ephemeral_node_path = ""

    def create_ephemeral_node_if_not_exists(self):
        if self.ephemeral_node_path == "":
            self.ephemeral_node_path = self.kzclient.create_node(
                self.leader_election_znode_root_path + self.leader_node_name_prefix,
                hip.get_host_ip() + ":" + self.broker_port,
                True, True)

    def try_elect_leader(self, leader_election_callback, broker_port):
        self.broker_port = broker_port
        self.create_ephemeral_node_if_not_exists()
        self.leader_election_callback = leader_election_callback
        child_nodes = self.kzclient.get_children(self.leader_election_znode_root_path)
        if child_nodes is None or len(child_nodes) < 1:
            print("Nodes count should be  >= 1, at least there should be one active broker, exiting the application!")
            os._exit(0)

        child_nodes.sort()
        print("Sorted nodes for the leader election: {}".format(child_nodes))
        # if the created ephemeral node path is the node with smallest sequence number
        # Then this broker is the leader, so don't follow any other nodes
        node_to_watch = ""
        if self.ephemeral_node_path.endswith(child_nodes[0]):
            print("This node is the leader: {}".format(self.ephemeral_node_path))
            node_to_watch = self.ephemeral_node_path

        else:
            this_broker_node_name = "broker_" + self.ephemeral_node_path[len(self.ephemeral_node_path) - 10:]
            this_broker_node_index = child_nodes.index(this_broker_node_name)
            # watch for the broker with the next lower index e.g. broker "broker_0000000003" will watch "broker_0000000002"
            # broker "broker_0000000002" will watch "broker_0000000001"
            # and broker "broker_0000000001" will not watch anyone, as it's the leader
            node_to_watch_index = this_broker_node_index - 1
            node_being_followed = self.leader_election_znode_root_path + '/' + child_nodes[node_to_watch_index]
            node_to_watch = node_being_followed
        self.kzclient.watch_individual_node(node_to_watch, self.watch_for_delete)

        if self.leader_election_callback != None:
            self.leader_election_callback(
                self.kzclient.get_node_value(self.leader_election_znode_root_path + "/" + child_nodes[0]))

    def watch_for_delete(self, event):
        print("There's a change event in the leader node event_type:{}".format(event.type))
        if event.type == "DELETED":
            self.try_elect_leader(self.leader_election_callback, self.broker_port)
