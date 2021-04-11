import zmq
import sys
import zk_clientservice as kzcl
import zk_leaderelector as le
import constants as const
import time
import multiprocessing as mp
import os
import host_ip_provider as hip

# The process to run the subscribers
process_list = []
brokers_root_path = "/brokers"
brokers_ip_port_root_path = "/brokers_ipport"
zkclient = kzcl.ZkClientService()
this_broker_node_path = ""


def run_broker(listening_port, publishing_port):
    print("starting ZMQ broker")
    try:
        context = zmq.Context(1)
        # Socket facing clients
        frontend = context.socket(zmq.SUB)
        frontend.bind("tcp://*:{}".format(listening_port))
        print("configured to listen to publisher interfaces via port {}".format(listening_port))

        frontend.setsockopt_string(zmq.SUBSCRIBE, "")

        # Socket facing services
        backend = context.socket(zmq.PUB)
        backend.bind("tcp://*:{}".format(publishing_port))
        print("configured to publish to registered subscribers via port {}".format(publishing_port))

        zmq.device(zmq.FORWARDER, frontend, backend)
        print("configuration complete")
    except Exception as e:
        print(e)
        print("bringing down ZMQ device")
    finally:
        pass
        frontend.close()


def create_broker_node():
    global this_broker_node_path
    this_broker_node_path = zkclient.create_node(brokers_root_path + "/" + "broker_", "0", True, True)
    this_broker_node_name = get_this_broker_name(this_broker_node_path)
    host_ip = hip.get_host_ip()
    zkclient.create_node(brokers_ip_port_root_path + "/" + this_broker_node_name,
                         host_ip + ":" + listen + "," + publish, True)
    print("This broker node path is {}".format(this_broker_node_path))
    zkclient.watch_individual_node(this_broker_node_path, watch_broker_node_change)


def get_this_broker_name(this_broker_node_path):
    return this_broker_node_path[len(this_broker_node_path) - 17:]


def watch_broker_node_change(event):
    if event.type == "DELETED":
        if len(process_list) > 0:
            thr = process_list[0]
            thr.terminate()
            process_list.pop(0)
        return
    broker_node_value = zkclient.get_node_value(this_broker_node_path)
    if broker_node_value == "1":
        print("Promoting {} as active broker".format(this_broker_node_path))
        if len(process_list) > 0:
            thr = process_list[0]
            thr.terminate()
            process_list.pop(0)
        thr = mp.Process(target=run_broker, args=(listen, publish))
        process_list.append(thr)
        thr.start()
    elif broker_node_value == "0":
        print("Loadbalancer deactivated the broker {}".format(this_broker_node_path))
        if len(process_list) > 0:
            thr = process_list[0]
            thr.terminate()
            process_list.pop(0)
    zkclient.watch_individual_node(this_broker_node_path, watch_broker_node_change)


# extract broker config
listen = sys.argv[1] if len(sys.argv) > 1 else print("Please submit valid port")
publish = sys.argv[2] if len(sys.argv) > 2 else print("Please submit valid port")
if publish == listen:
    print("Listening port and publishing port cannot be the same")
    os._exit(0)
else:
    create_broker_node()
    zkclient.watch_individual_node(this_broker_node_path, watch_broker_node_change)

while True:
    time.sleep(1000000000)
