import zmq
import uuid
from datetime import datetime
import host_ip_provider as hip
import random


class DirectPubMiddleware():
    def __init__(self, port):
        self.port = port
        self.socket = None
        self.configure()

    def configure(self):
        print("configuring the publisher middleware to use the direct strategy")
        context = zmq.Context()

        binding_address = "tcp://*:%s" % self.port

        # acquire a publisher type socket
        print("Publisher middleware binding on all interfaces on port {}".format(self.port))
        self.socket = context.socket(zmq.PUB)
        self.socket.bind(binding_address)

    def publish(self, topic, value):
        print("publishing topic: {}, data: {}".format(topic, value))
        message_id = str(uuid.uuid4())
        message_sent_at_timestamp = datetime.now().strftime('%Y-%m-%dT%H::%M::%S.%f')
        host_ip = hip.get_host_ip()
        # topic:data:message_id:message_sent_at_timestamp
        published_data = topic + "#" + str(value) + "#" + message_id + "#" + message_sent_at_timestamp + '#' + host_ip
        self.socket.send_string(published_data)
