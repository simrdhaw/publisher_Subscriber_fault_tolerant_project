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

file_path_broker_data = 'broker_data.json'
file_path_replica_data = 'replica_data.json'

ops_fid = None

class MessageBroker:
    def __init__(self , broker_id, broker_ip, is_primary):
        #self.num_replicas = len(replicas) + 1;
        self.broker_id = broker_id
        self.broker_ip = broker_ip
        self.is_primary = is_primary
        self.is_alive = True
    
    def __str__(self):
        return json.dumps({"id": self.broker_id, "ip": self.broker_ip, "is_primary": self.is_primary})


class Operation:
    def __init__(self, flask_method, broker_function,params,operation):
        self.flask_method = flask_method
        self.broker_function = broker_function
        self.params = params
        self.operation = operation
    def __str__(self):
        return json.dumps({"flask_method": self.flask_method, "broker_function": self.broker_function, "params": self.params})
    
    def to_local_operation_json(self):
        d = {k: self.params[k] for k in self.params}
        d["operation"] = self.operation
        return json.dumps(d)

    def write_to_log(self):
        ops_fid.write(self.to_local_operation_json())
        ops_fid.write('\n')
        ops_fid.flush()
    
    def replicate(self):
        for replica in replicas:
        # if replica.is_primary:
        #     continue
            if replica.is_alive:
                try:
                    replication_response = requests.post(
                        "http://{ip}/{method}".format(ip=replica.broker_ip, method=self.flask_method), 
                        params=self.params)
                    # Check response as well
                    print('replication response: ',  replication_response)
                except requests.RequestException:
                    replica_down[replica.broker_ip].append(self)
                    print("len of replica_down : {0}".format(len(replica_down[replica.broker_ip])));
            else:
                replica_down[replica.broker_ip].append(self)
                print("len of replica_down (else code) : {0}".format(len(replica_down[replica.broker_ip])));

class UserTopicState:
    def __init__(self):
        self.topic_to_index = {}
    
    def add_subscribed_topic(self, topic):
        if topic not in self.topic_to_index:
            if topic in message_queues:
                self.topic_to_index[topic] = 0 #len(message_queues[topic])
            else:
                self.topic_to_index[topic] = 0;

    def get_subscribed_topics(self):
        for topic in self.topic_to_index:
            yield (topic, self.topic_to_index[topic]) 
    
    def set_index_of_topic(self, topic,idx):
        self.topic_to_index[topic] = idx
    
    def get_index_of_topic(self,topic):
        return self.topic_to_index[topic]



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
    stored_op = Operation("post", "replicate/publish",{'topic': topic, 'data': data}, "publish")
    stored_op.write_to_log()
    stored_op.replicate()

    response = jsonify({'message': "Published topic {0} data {1}".format(topic, data)})
    response.status_code = 200
    return response
    


@app.route("/")
def hello_world():
    return "<p>Welcome to our Publish-Subscribe system.</p>"

@app.route('/stream')
def stream_messages():
    user = request.args["username"]
    if user not in user_to_subscribed_topics:
        response = jsonify({'message': "User {0} is not registered.".format(user)})
        response.status_code = 404
        return response

    def event_stream():
        yield 'data: Listening to new message for subscribed topics \n\n'
        for _ in range(1000):
            # wait for source data to be available, then push it
            for topic,idx in  user_to_subscribed_topics[user].get_subscribed_topics():
                if topic not in message_queues:
                    continue
                length_of_queue = len(message_queues[topic])
                for j in range(idx, length_of_queue):
                    message = 'data: {0}\n\n'.format(message_queues[topic][j]) 
                    yield message
                    stored_op = Operation("post", "replicate/stream",{'username': user, 'topic': topic, 'idx': j}, "stream")
                    stored_op.write_to_log()
                    stored_op.replicate()
                user_to_subscribed_topics[user].set_index_of_topic(topic, length_of_queue)                   
            time.sleep(1)
    
    return Response(event_stream(), mimetype="text/event-stream")

@app.route('/subscribe', methods=['POST'])
def add_subscriber():
    user = request.args["username"]
    topic = request.args["topic"]
    subscribe_user_internal(user,topic)
    stored_op = Operation("post", "replicate/subscribe",{'username': user, 'topic': topic}, "subscribe")
    stored_op.write_to_log()
    stored_op.replicate()

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

    stored_op = Operation("post", "replicate/add_replica",{'broker_id': id, 'broker_ip': ip, 'is_primary': is_primary}, "replica")
    stored_op.write_to_log()

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

    stored_op = Operation("post", "initialize_broker",{'broker_id': id, 'broker_ip': ip, 'is_primary': is_primary}, "initialize_broker")
    stored_op.write_to_log()

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
    user = request.args["username"]
    topic = request.args["topic"]
    subscribe_user_internal(user,topic)
    stored_op = Operation("post", "replicate/subscribe",{'username': user, 'topic': topic}, "subscribe")
    stored_op.write_to_log()
    stored_op.replicate()
    response = jsonify({'message': "Successfully subscribed {0} to topic {1}".format(user, topic)})
    response.status_code = 200
    return response

@app.route('/replicate/publish', methods=['POST'])
def replicate_publish_topic():
    topic = request.args["topic"]
    data = request.args["data"]
    publish_topic_internal(topic, data)
    stored_op = Operation("post", "replicate/publish",{'topic': topic, 'data': data}, "publish")
    stored_op.write_to_log()
    stored_op.replicate()
    response = jsonify({'message': "Published topic {0} data {1}".format(topic, data)})
    response.status_code = 200
    return response

@app.route('/replicate/stream')
def copy_messages_sent():
    user = request.args["username"]
    topic = request.args["topic"]
    idx = request.args["idx"]
    user_to_subscribed_topics[user].set_index_of_topic(topic, idx+1)
    stored_op = Operation("post", "replicate/stream",{'username': user, 'topic': topic, 'idx': idx}, "stream")
    stored_op.write_to_log()
    stored_op.replicate()
    response = jsonify({'message': " topic {0} idx {1} username {2}".format(topic, idx, user )})
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
               
                if replica.broker_ip in replica_down:
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

def initialize_from_stored_ops(port):
    try:
        with open("ops_{0}".format(args.port), "r") as fid:
            for line in fid:
                d = json.loads(line)
                if d['operation'] == "publish":
                    topic = d["topic"]
                    data = d["data"]
                    publish_topic_internal(topic, data)
                elif d['operation'] == "subscribe":
                    user = d["username"]
                    topic = d["topic"]
                    subscribe_user_internal(user,topic)
                elif d['operation'] == "stream":
                    user = d["username"]
                    topic = d["topic"]
                    idx = d["idx"]
                    user_to_subscribed_topics[user].set_index_of_topic(topic, idx+1)
                elif d['operation'] == "replica":
                    global replicas
                    id = d["broker_id"]
                    ip = d["broker_ip"]
                    is_primary = bool(int(d["is_primary"]))
                    replicas.append(MessageBroker(id, ip, is_primary))
                elif d['operation'] == "initialize_broker":
                    id = d["broker_id"]
                    ip = d["broker_ip"]
                    is_primary = bool(int(d["is_primary"]))
                    global broker
                    broker = MessageBroker(id, ip, is_primary)
                else:
                    print("Found unexpected operation: ", line)
                    exit(1)
    except FileNotFoundError as e:
        print("Previous state not found, this is a fresh broker.")


        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', dest='port')
    args = parser.parse_args()
    initialize_from_stored_ops(args.port)

    #global ops_fid
    ops_fid = open("ops_{0}".format(args.port), "a")
    if ops_fid is None:
        # create file
        pass

    thread = Thread(target = send_my_heartbeat)
    thread.start()
    
    #print("Dhawan")
    webserverThread = Thread(target = run_webserver,args=(args.port,) )
    webserverThread.start()
    
    thread.join()
    webserverThread.join()