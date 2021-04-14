import json


class topic_trackers():
    def __init__(self):
        self.topic_pubs = {}
        self.topic_subs = {}

    def set_topic_pubs(self, topicname, publisher):
        if topicname in self.topic_pubs:
            pubs = self.topic_pubs[topicname]
            self.topic_pubs[topicname] = pubs.append(publisher)
        else:
            self.topic_pubs[topicname] = [publisher]

    def set_topic_subs(self, topicname, subscriber):
        if topicname in self.topic_subs:
            subs = self.topic_subs[topicname]
            self.topic_subs[topicname] = subs.append(subscriber)
        else:
            self.topic_subs[topicname] = [subscriber]

    def get_topic_pubs(self, topicname):
        if topicname in self.topic_pubs:
            pubs = self.topic_pubs[topicname]
            return pubs
        else:
            return None

    def get_topic_subs(self, topicname):
        if topicname in self.topic_subs:
            pubs = self.topic_subs[topicname]
            return pubs
        else:
            return None


def main():
    print("inside main")
    topic_tracker_obj = topic_trackers()
    topic_tracker_obj.set_topic_pubs("t1", "pub1")
    topic_tracker_obj.set_topic_pubs("t1", "pub2")
    topic_tracker_obj.set_topic_subs("t1", "sub1")
    topic_tracker_obj.set_topic_subs("t1", "sub2")
    print(json.dumps(topic_tracker_obj.__dict__))


if __name__ == '__main__':
    main()
