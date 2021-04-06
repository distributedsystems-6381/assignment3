import sys
import zmq


class BrokerSubMiddleware():
    def __init__(self, brokers):
        self.brokers = brokers
        self.notifyCallback = None
        self.sockets = []
        self.registered_topics = None

    def register(self, topics, callback=None):
        self.registered_topics = topics
        self.notifyCallback = callback
        self.configure()

    def configure(self):
        print("configuring the subscriber middleware to use the broker strategy")
        context = zmq.Context()

        # for all of the registered topics set filter
        for topic in self.registered_topics:
            for broker in self.brokers:
                server_address = "tcp://{}".format(broker)
                print("Connecting publisher server at address: {}".format(server_address))
                socket = context.socket(zmq.SUB)
                socket.connect(server_address)
                socket.setsockopt_string(zmq.SUBSCRIBE, topic)
                self.sockets.append(socket)

        # keep polling for the sockets
        poller = zmq.Poller()
        for socket in self.sockets:
            poller.register(socket, zmq.POLLIN)

        while True:
            sockets = dict(poller.poll())
            for socket in sockets:
                message = socket.recv_string()
                find_val = message.find('#')
                topic = message[0:find_val]
                messagedata = message[find_val + 1:]
                # send the received data to the subscriber app using the registered callback
                if self.notifyCallback != None:
                    self.notifyCallback(topic, messagedata)
