# Distributed System(6381) Assignment3- Load balacing, ownership strength, samples match for history QoS
##### Group members
Satish & Tito
##### Pre-requisites
   - [Ubuntu 20.04 machine](https://ubuntu.com/download/desktop)
   - [Mininet](https://github.com/mininet/mininet)
   - [ZeroMQ](https://zeromq.org/)
   - [Python3](https://www.python.org/)
   - [Java](https://www.java.com/en/)
   - [Zookeeper](https://zookeeper.apache.org/releases.html#download)

**Install python3, Zeromq, Java, and Zookeeper on your Ubuntu machine with the below command:**
   - we assume that mininet is already installed
```
sudo apt-get update && \
sudo apt-get install python3-dev python3-pip && \
sudo -H python3 -m pip install --upgrade pyzmq && \
python3 -V && \
sudo apt install openjdk-11-jdk && \
java -version && \
cd ~ && mkdir workspace && cd workspace && \
wget https://mirrors.ocf.berkeley.edu/apache/zookeeper/zookeeper-3.6.2/apache-zookeeper-3.6.2-bin.tar.gz -P ~/workspace && \
tar xvzf apache-zookeeper-3.6.2-bin.tar.gz && \
mv apache-zookeeper-3.6.2-bin zookeeper && \
./zookeeper/bin/zkServer.sh version
```
  - to use mininet with python3 for automation, follow the below steps:
```
git clone https://github.com/mininet/mininet.git && \
cd mininet && \
util/install.sh -a && \
PYTHON=python3 util/install.sh -fnv && \
python3 `which mn`
```

**High Level Design:**

![alternativetext](/assignment3-hight-level-design.png)

***To run Zookeeper - required for leader election and fail-over of brokers***
1. change directories to the zookeeper installation - in the case shown above 
   - `~/workspace/zookeeper/`
1. execute the first two commands below if you are working with a brand new install of zookeeper
   - you can modify the default config you like, but it will work fine without doing so 
      - `cp conf/zoo_sample.cfg conf/zoo.cfg`
      - `mv conf/zoo_sample.cfg conf/zoo_sample_conf`
1. start zookeeper, we will use this command from a mininet host
   - be sure to note the IP of the host that you choose so that the publishers and subscribers can contact zookeeper
     - `sudo ./bin/zkServer.sh start-foreground`

***To run the publisher and subscriber - direct implementation:***
1. Change directories to your workspace and clone this project 
   ```
   cd ~/workspace
   git clone https://github.com/distributedsystems-6381/assignment2-faulttolerant-pubsub
   cd assignment2-faulttolerant-pubsub
   ```
1. Run publishers by executing the below commnad:  
     ```
      python3 publisher_app.py direct "{zookeeper_ip_port}" "{publisher's_topics_publishing_port}" "{topic_name_1}" "{topic_name_2}"....."{topic_name_n}"
     ```
      e.g.:
     ```
     python3 publisher_app.py direct "10.0.0.1:2181" "2000" "topic1" "topic2"
     ```
1. Run lamebrokers by executing below command:  
     ```
     python3 lamebroker.py {broker_req_response_binding_port}
     ```
     e.g.:
     ```
     python3 lamebroker.py 3000
     ```
 1. Run subscriber by running command:    
     ```
     python3 subscriber_app.py direct "{zookeeper_ip_port}" "{topic_name_1}" "{topic_name_2}"....."{topic_name_n}"
     ```
     e.g.:
     ```
     python3 subscriber_app.py direct "10.0.0.1:2181" "topic1"
     ``` 
***Command to run brokers, publishers and subscribers - broker implementation:***
1. Change directories to your workspace and clone this project 
   ```
   cd ~/workspace
   git clone https://github.com/distributedsystems-6381/assignment2-faulttolerant-pubsub
   cd assignment2-faulttolerant-pubsub
   ```
1. Run brokers by executing the below commnad:  
     ```
      python3 broker.py "{listening_port_for_the_publishers}" "{publishing_port_for_the_subscribers}"
     ```
      e.g.:
     ```
      python3 broker.py 2000 2001
     ```
1. Run publishers by executing below command:  
     ```
     python3 publisher_app.py broker "{zookeeper_ip_port}" "{topic_name_1}" "{topic_name_2}"....."{topic_name_n}"
     ```
     e.g.:
     ```
     python3 publisher_app.py broker "10.0.0.1:2181" "topic1" "topic2"
     ```
1. Run subscriber by running below command:     
     ```
     python3 subscriber_app.py broker "{zookeeper_ip_port}" "{topic_name_1}" "{topic_name_2}"....."{topic_name_n}"
     ```
     e.g.:
     ```
     python3 subscriber_app.py broker "10.0.0.1:2181" "topic1"
     ``` 
     
_**NOTES**_
   - By default, the publisher app publishes messages to 2 topics
   - Temperature (temp) and humidity by calling the method `publish(topic, message)` via the publisher middleware API
   - To publish the data to any other topics, please include the additional topic parameter, and there will be rnadom data between 100 and 200 will be published for these topics.
   - The subscriber registers the topics via the subscriber middleware API and uses a callback method to receive the data for the registered topics
   - When the subscriber middleware receives topic data from the publisher related to the subscriber's registered topics, it passes the data to subscriber app by calling the registered callback function

***Test Scenarios for Direct Implementation:***
- Start one publisher publishing "topic1" and "topic2"
- Run two instances of the lamebroker process
- Start a subscriber listening for "topic2"
- Start another publisher publishing "topic2", lamebroker will refresh the list of publishers
- Kill one broker process by keyboard interrupt e.g. "Ctrl + C", the second broker will resume the role of the leader	
- The subscriber will be notified of the broker change and will refresh the publishers from the new broker
- Stop all brokers, the subscriber will be notified of no brokers in the system and will shut down
	
***Test Scenarios for Broker Implementation:***
- Start two brokers, the broker with minimum sequence node will become the leader
- Run two instances of the publishers publishing "topic1" and "topic2"
- Start a subscriber listening for "topic2"
- Stop the current leader broker process, the second broker will become the leader
- The topic publishers and subscribers will be notified of the broker change and will reconnect to the new broker node and start receiving the topics			- 
- Stop all brokers, the subscriber will shut down with the message there's no broker in the system	

***Logging and Graph:*** 
   Running subscriber app generates a comma seperated log text file at the root of the project containing publisher_ip, subscriber_ip, message_id and time taken in milliseconds to receive the message from publisher. The graph and it's test data for the above test scenarios are located in the folder /perf-data-graphs
