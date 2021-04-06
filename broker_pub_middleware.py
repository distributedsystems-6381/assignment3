import zmq
import uuid
from datetime import datetime
import host_ip_provider as hip


class BrokerPubMiddleware():
    def __init__(self, broker):
        self.address = broker
        self.socket = None
        self.configure()

    def configure(self):
        print("configuring the publisher middleware to use the broker strategy")
        context = zmq.Context()
        self.socket = context.socket(zmq.PUB)
        binding_address = "tcp://{}".format(self.address)
        self.socket.connect(binding_address)
        print("will send messages to broker at: {}".format(self.address))

    def publish(self, topic, value):
        print("publishing topic: {}, data: {}".format(topic, value))
        message_id = str(uuid.uuid4())
        message_sent_at_timestamp = datetime.now().strftime('%Y-%m-%dT%H::%M::%S.%f')
        host_ip = hip.get_host_ip()
        # topic:data:message_id:message_sent_at_timestamp
        published_data = topic + "#" + str(value) + "#" + message_id + "#" + message_sent_at_timestamp + '#' + host_ip
        self.socket.send_string(published_data)
