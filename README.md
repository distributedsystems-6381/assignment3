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

***Load balancing threshold***
- If a topic gets published and subscribed by > 1 publisher and subscriber respectively, the broker intances are scaled out by the load balancer
- The broker is scaled in if a topic publisher or subscriber becomes < 1

***Load balancing scenario***
1. Change directories to your workspace and clone this project 
   ```
   cd ~/workspace
   git clone https://github.com/distributedsystems-6381/assignment3.git
   cd assignment3
   ```
1. Run load balancer by executing the below commnad:  
     ```
      python3 load_balancer.py
     ```    
1. Run 3 replica of the broker
     ```
      python3 broker.py "{listening_port_for_the_publishers}" "{publishing_port_for_the_subscribers}"
     ```
      e.g.:
     ```
      python3 broker.py 2000 2001
     ```
 1. Run 2 instances of "topic1" publisher by running command:    
     ```
     python3 subscriber_app.py direct "{zookeeper_ip_port}" "{topic_name_1}"
     ```
     e.g.:
     ```
     python3 publisher_app.py broker "27.0.0.1:2181" "topic1"
     ``` 
  1. Run 1 instances of "topic1" subscriber by running command
      ```
     python3 subscriber_app.py broker "{zookeeper_ip_port}" "{topic_name_1}" "{topic_name_2}"....."{topic_name_n}"
     ```
     e.g.:
     ```
     python3 subscriber_app.py broker "10.0.0.1:2181" "topic1"
     ``` 
     ```
     Note: Notice that there's is still only one active broker instance
     ```
  1. Run another instance of "topic1" subscriber by running command
     ```
     python3 subscriber_app.py broker "{zookeeper_ip_port}" "{topic_name_1}" "{topic_name_2}"....."{topic_name_n}"
     ```
     e.g.:
     ```
     python3 subscriber_app.py broker "10.0.0.1:2181" "topic1"
     ``` 
     ```
     Note: Notice that now there will be 2 active broker becuse topic1 is published and subscribed by > 1 publishers and subscribers
     ```
  1. Terminate publisher and/or subscriber by pressing Ctrl+C:   
     ```
     Note: Notice that the second instance of the broker will be deactivated
     ```

***Ownership strength scenario***
- Run load balancer
- Run at leasr 1 broker
- Start one publisher publishing "topic1"
- Start another publisher publishing "topic1"
- Start a subscriber listening for "topic1"
    ```
    Note: please notice that the subscriber will receive messages from the publisher having higher ownership strenth i.e the one who started publishing topic1 earlier
    ```
***Samples History QoS scenario***
- Run load balancer
- Run at leasr 1 broker
- Start one publisher publishing "topic1:10" (note: 10 is the history QoS samples)
- Start a subscriber listening for "topic1:20"
    ```
    Note: please notice that the subscriber doesn't receive the messages
    ```
- Stop the subscriber and start again subscribing "topic1:10"
    ```
    Note: please notice that the subscriber starts receving the messages now
    ```
***Logging and Graph:*** 
   Running subscriber app generates a comma seperated log text file at the root of the project containing publisher_ip, subscriber_ip, message_id and time taken in milliseconds to receive the message from publisher. The graph and it's test data for the above test scenarios are located in the folder /perf-data-graphs
