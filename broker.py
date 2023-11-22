from flask import Flask, request, jsonify, Response
import requests
import time
import json
from threading import Thread 
import argparse
from collections import defaultdict

app = Flask(__name__)

app.name = "message_broker"
message_queues = {} #topic : idx0 ,idx 1,idx 2
# {user}:{topic1} -idx
user_to_subscribed_topics = {}


replicas = []
broker = []
#replica_down = {}
replica_down = defaultdict(list)
class Operation:
    def __init__(self, flask_method, broker_function,params):
        self.flask_method = flask_method
        self.broker_function = broker_function
        self.params = params
    def __str__(self):
        #return "id: {0}, ip: {1}, is_primary: {2}".format(self.broker_id, self.broker_ip ,self.is_primary)
        return json.dumps({"flask_method": self.flask_method, "broker_function": self.broker_function, "params": self.params})

class UserTopicState:
    def __init__(self):
        self.topic_to_index = {}
    
    def add_subscribed_topic(self, topic):
        if topic not in self.topic_to_index:
            self.topic_to_index[topic] = 0

    def get_subscribed_topics(self):
        for topic in self.topic_to_index:
            yield (topic, self.topic_to_index[topic]) 
    
    def set_index_of_topic(self, topic, idx):
        self.topic_to_index[topic] = idx
    

class MessageBroker:
    def __init__(self , broker_id, broker_ip, is_primary):
        #self.num_replicas = len(replicas) + 1;
        self.broker_id = broker_id
        self.broker_ip = broker_ip
        self.is_primary = is_primary
        self.is_alive = True
    
    def __str__(self):
        #return "id: {0}, ip: {1}, is_primary: {2}".format(self.broker_id, self.broker_ip ,self.is_primary)
        return json.dumps({"id": self.broker_id, "ip": self.broker_ip, "is_primary": self.is_primary})


def publish_topic_internal(topic, data):
    if topic not in message_queues:
        message_queues[topic] = []
    message_queues[topic].append(data)

def subscribe_user_internal(user,topic):
    if user not in user_to_subscribed_topics:
        user_to_subscribed_topics[user] = UserTopicState()
    user_to_subscribed_topics[user].add_subscribed_topic(topic)

@app.route('/publish', methods=['POST'])
def publish_topic():
    topic = request.args["topic"]
    data = request.args["data"]
    publish_topic_internal(topic, data)
    
    for replica in replicas:
        if replica.is_primary:
            continue
        print(replica)
        if replica.is_alive:
            try:
                replication_response = requests.post(
                    "http://" + replica.broker_ip+"/replicate/publish", 
                    params={'topic': topic, 'data': data})
                # Check response as well
                print('replication response: ',  replication_response)
            except requests.RequestException:
                stored_op = Operation("post", "replicate/publish",{'topic': topic, 'data': data});
                print(str(stored_op))
                replica_down[replica.broker_ip].append(stored_op)
                print("len of replica_down at publisher : {0}".format(len(replica_down[replica.broker_ip])));
        else:
            stored_op = Operation("post", "replicate/publish",{'topic': topic, 'data': data});
            replica_down[replica.broker_ip].append(stored_op)
            print("len of replica_down (else code) at publisher : {0}".format(len(replica_down[replica.broker_ip])));
            #print(str(stored_op))

    response = jsonify({'message': "Published topic {0} data {1}".format(topic, data)})
    response.status_code = 200

    return response
    


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route('/stream')
def stream_messages():
    user = request.args["username"]
    if user not in user_to_subscribed_topics:
        response = jsonify({'message': "User {0} is not registered.".format(user)})
        response.status_code = 404
        return response

    def event_stream():
        yield 'data: Listening to new message for subscribed topics \n\n'
        for i in range(1000):
            # wait for source data to be available, then push it
            for topic,idx in  user_to_subscribed_topics[user].get_subscribed_topics():
                if topic not in message_queues:
                    continue
                for j in range(idx, len(message_queues[topic])):
                    yield 'data: {}\n\n'.format(message_queues[topic][j])
                user_to_subscribed_topics[user].set_index_of_topic(topic, len(message_queues[topic]))
            time.sleep(1)

    return Response(event_stream(), mimetype="text/event-stream")

@app.route('/subscribe', methods=['POST'])
def add_subscriber():
    print("args: ", request.args);
    user = request.args["username"]
    topic = request.args["topic"]
    
    subscribe_user_internal(user,topic)

    ### Send replication message
    #print('replicas: ', replicas)
    for replica in replicas:
        if replica.is_alive:
            try:
                replication_response = requests.post(
                    "http://" + replica.broker_ip +"/replicate/subscribe", 
                    params={'username': user, 'topic': topic})
            except requests.RequestException:
                stored_op = Operation("post", "replicate/subscribe",{'username': user, 'topic': topic})
                print(str(stored_op))
                replica_down[replica.broker_ip].append(stored_op)
                print("len of replica_down at subscriber : {0}".format(len(replica_down[replica.broker_ip])))
        else:
            stored_op = Operation("post", "replicate/subscribe",{'username': user, 'topic': topic});
            replica_down[replica.broker_ip].append(stored_op)
            print("len of replica_down (else code) at subscriber : {0}".format(len(replica_down[replica.broker_ip])));
               

    response = jsonify({'message': "Successfully subscribed {0} to topic {1}".format(user, topic)})
    response.status_code = 200
    return response



### Replication endpoints

@app.route('/replicate/add_replica', methods=['POST'])
def add_replica():
    print("replicated broker: ", request.args);
    id = request.args["broker_id"]
    ip = request.args["broker_ip"]
    is_primary = bool(int(request.args["is_primary"]))
    replicas.append(MessageBroker(id, ip, is_primary))
    print("Replicated broker added by appending")
    response = jsonify({'message': "Successfully added replicated broker {0}".format(ip)})
    response.status_code = 200
    print('replicas: ', replicas)

    return response

@app.route('/list_replicas', methods=['GET'])
def list_replicas():
    print("Available replicas are posted")
    print(broker)
    replicas_and_me = []
    replicas_and_me.append(broker)
    replicas_and_me.extend([r for r in replicas])
    
    response = jsonify({'replicas': [str(r) for r in replicas_and_me]})
    response.status_code = 200
    return response

@app.route('/initialize_broker', methods=['POST'])
def initialize_broker():
    id = request.args["broker_id"]
    ip = request.args["broker_ip"]
    is_primary = bool(int(request.args["is_primary"]))
    global broker
    broker = MessageBroker(id, ip, is_primary)
    response = jsonify({'broker': str(broker)})
    response.status_code = 200
    return response




@app.route('/heartbeat/receive', methods=['POST'])
def receive_heartbeat():
    ip = request.args["broker_ip"]
    response = jsonify({'message': "Receiving heartbeat with ip {0}".format(ip)})
    response.status_code = 200
    print("heartbeat recieved from ip {0}".format(ip))
    return response

@app.route('/replicate/subscribe', methods=['POST'])
def replicate_add_subscriber():
    print("args: ", request.args);
    user = request.args["username"]
    topic = request.args["topic"]

    subscribe_user_internal(user,topic)

    response = jsonify({'message': "Successfully subscribed {0} to topic {1}".format(user, topic)})
    response.status_code = 200

    return response

@app.route('/replicate/publish', methods=['POST'])
def replicate_publish_topic():
    print("args: ", request.args);
    topic = request.args["topic"]
    data = request.args["data"]

    publish_topic_internal(topic, data)

    response = jsonify({'message': "Published topic {0} data {1}".format(topic, data)})
    response.status_code = 200

    return response

def run_operations(broker_ip):
    #global replica_down
    for oper in replica_down[broker_ip]:
        if oper.flask_method == "post":
            response = requests.post(
                "http://" + broker_ip +"/"+ oper.broker_function,
                params=oper.params),
            print("Sent post request on upping a replica")
        elif oper.flask_method == "get":
            reponse = requests.get(
                "http://" + broker_ip +"/"+ oper.broker_function,
                params=oper.params
            )
        else:
            print("invalid operation generated")


def send_my_heartbeat():
    print("entered start heartbeat")
    global broker
    global replica_down
    while(1):
        for replica in replicas:
            try:
                heartbeat_response = requests.post(
                    "http://" + replica.broker_ip +"/"+ "/heartbeat/receive",
                    params={'broker_ip': broker.broker_ip})
                #print("Simran:{0}".format(heartbeat_response))
                replica.is_alive = True;
               
                for replica.broker_ip in replica_down:
                    print("enter replica_down {0}".format(len(replica_down[replica.broker_ip])))
                    run_operations(replica.broker_ip)
                    replica_down[replica.broker_ip] = []
                #print("broker_ip {0} is dead".format(replica.broker_ip) )
            except requests.RequestException:
                replica.is_alive = False;
                print("broker_ip {0} is dead".format(replica.broker_ip) )
        
        time.sleep(5) 
    

def run_webserver(port):
    print("starting flask")
    app.run(host="0.0.0.0", port=port, threaded=True)

if __name__ == '__main__':
    #print("SIMRAN")
    thread = Thread(target = send_my_heartbeat)
    thread.start()
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', dest='port')
    args = parser.parse_args()
    #print("Dhawan")
    webserverThread = Thread(target = run_webserver,args=(args.port,) )
    webserverThread.start()
    
    thread.join()
    webserverThread.join()