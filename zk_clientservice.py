import sys
import logging as logger

import kazoo.client as kzcl
import kazoo.retry as kzretry
import kazoo.exceptions as ke
import constants as const


class ZkClientService():
    def __init__(self):
        _retry = kzretry.KazooRetry(max_tries=1000, delay=0.5, backoff=2)
        self.kzclient = kzcl.KazooClient(const.ZOO_KEEPER_SERVER_IP_PORT, connection_retry=_retry)
        self.kzclient.start()

    def create_node(self, node_path_to_create, node_value, is_ephemeral, is_sequential):
        print("Creating node")
        created_node_path = ""
        try:
            node_stat = self.kzclient.exists(node_path_to_create)
            if node_stat is None:
                created_node_path = self.kzclient.create(node_path_to_create, node_value.encode('utf-8'), makepath=True,
                                                         ephemeral=is_ephemeral, sequence=is_sequential)
            else:
                created_node_path = node_path_to_create

        except ke.KazooException as e:
            logger.error('Kazoo exception: ' + str(e))

        except Exception as e:
            logger.error('Exception occured: ' + str(e))

        return created_node_path

    def watch_node(self, node_path, watch_func):
        try:
            if self.kzclient.exists(node_path):
                self.kzclient.get_children(node_path, watch=watch_func)

        except ke.KazooException as e:
            logger.error('Kazoo exception: ' + str(e))

        except Exception as e:
            logger.error('Exception occured: ' + str(e))

    def watch_individual_node(self, node_path, watch_func):
        try:
            if self.kzclient.exists(node_path):
                self.kzclient.get(node_path, watch=watch_func)

        except ke.KazooException as e:
            logger.error('Kazoo exception: ' + str(e))

        except Exception as e:
            logger.error('Exception occured: ' + str(e))

    def get_children(self, node_path):
        childNodes = None
        try:
            if self.kzclient.exists(node_path):
                childNodes = self.kzclient.get_children(node_path)
        except ke.KazooException as e:
            logger.error('Kazoo exception: ' + str(e))
        except Exception as e:
            logger.error('Exception occured: ' + str(e))

        return childNodes

    def get_node_value(self, node_path):
        if self.kzclient.exists(node_path):
            data, _ = self.kzclient.get(node_path)
            if data is None:
                return ""
            return data.decode("utf-8")

    def set_node_value(self, node_path, node_value):
        if self.kzclient.exists(node_path):
            self.kzclient.set(node_path, node_value.encode("utf-8"))
        else:
            self.kzclient.create(node_path, node_value.encode('utf-8'), makepath=True)

    def get_broker(self, broker_root_node_path):
        if self.kzclient.exists(broker_root_node_path):
            child_nodes = self.kzclient.get_children(broker_root_node_path)
            child_nodes.sort()
            active_broker_node = child_nodes[0]
            return self.get_node_value(broker_root_node_path + '/' + active_broker_node)
        else:
            return ""

    def get_broker_node_name(self, broker_root_node_path):
        if self.kzclient.exists(broker_root_node_path):
            child_nodes = self.kzclient.get_children(broker_root_node_path)
            child_nodes.sort()
            if len(child_nodes) > 0:
                active_broker_node = child_nodes[0]
                return active_broker_node
            else:
                return ""
        else:
            return ""

    def stop_kzclient(self):
        self.kzclient.stop()
