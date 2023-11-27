import requests
import subprocess
import platform
import sys
import boto3
import json
import webbrowser

def get_running_aws_instances():
    # Create an EC2 client
    ec2 = boto3.client('ec2', region_name='us-east-2',aws_access_key_id='###', 
                aws_secret_access_key='###')  # Ensure your AWS credentials are configured
    # Retrieve information about running instances
    response = ec2.describe_instances()
    public_ips = []
    # Extract and display public IPv4 addresses of running instances
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            if 'PublicIpAddress' in instance and instance['State']['Name'] == 'running':
                public_ip = instance['PublicIpAddress']
                security_groups = instance['SecurityGroups']
                for sg in security_groups:
                    sg_id = sg['GroupId']
                    
                    # Describe the inbound rules of the security group
                    security_group = ec2.describe_security_groups(GroupIds=[sg_id])
                    for rule in security_group['SecurityGroups'][0]['IpPermissions']:
                        if 'FromPort' in rule:
                            from_port = rule['FromPort']
                            if from_port!=22:
                                public_ips.append(public_ip+':'+str(from_port))
    return public_ips

def find_primary_broker():
    public_ips = get_running_aws_instances()
    response = requests.get(f"http://{public_ips[0]}/list_replicas")
    
    response_data = response.json()  # Extract JSON data from the response

    # Accessing the 'replicas' key from the JSON data
    replicas = response_data.get('replicas', [])

    # Convert JSON strings to Python dictionaries
    replicas_data = [json.loads(replica) for replica in replicas]

    # Find the broker with is_primary=True
    primary_broker = next((json.loads(broker) for broker in replicas if json.loads(broker).get('is_primary', False)), None)

    if primary_broker and primary_broker['ip'] in public_ips:
            return primary_broker['ip']
    else:
        return None

def api_subscribe_user(username, broker_node,topic):
    active_nodes = get_running_aws_instances()
    broker_node = active_nodes[0]
    print("Primary broker is down ,updating the broker the primary broker to "+broker_node)
    response = requests.post(f"http://{broker_node}/subscribe?username={username}&topic={topic}")
    print(response.json())


def subscribe_user():
    global broker_node
    username = input("Enter username: ")
    topic = input("Enter topic to subscribe: ")
    broker_node = find_primary_broker()
    try:
        if broker_node is not None:
            print("Connecting to the primary broker...",broker_node)
            response = requests.post(f"http://{broker_node}/subscribe?username={username}&topic={topic}")
            print(response.json())
        else:
            api_subscribe_user(username, broker_node, topic)        
    except requests.RequestException:
        api_subscribe_user(username, broker_node, topic)

def api_stream_messages(broker, username):
    active_nodes = get_running_aws_instances()
    broker_node = active_nodes[0]
    print("Primary broker is down ,updating the primary broker to "+broker_node)
    response = requests.get(f"http://{broker_node}/stream?username={username}", stream=True)
    if response.status_code == 200:
        print("Opening window for streaming "+username+"'s messages..")
        webbrowser.open(f"http://{broker_node}/stream?username={username}")

#Function to stream the messages of the respective user
def stream_messages():
    global broker_node
    username = input("Enter username to stream messages: ")
    broker_node = find_primary_broker()
    try:
        if broker_node is not None:
            print("Connecting to the primary broker...",broker_node)
            response = requests.get(f"http://{broker_node}/stream?username={username}", stream=True)
            if response.status_code == 200:
                print("Opening window for streaming "+username+"'s messages..")
                webbrowser.open(f"http://{broker_node}/stream?username={username}")
        else:
            api_stream_messages(broker_node, username)
    except requests.RequestException:
        api_stream_messages(broker_node, username)     


def main():
    public_ips = get_running_aws_instances()
    print("List of available nodes..")
    print(public_ips)
    while True:
        print("\nMenu:")
        print("1. Subscribe User")
        print("2. Stream Messages")
        print("3. Exit")
        choice = input("Enter your choice: ")       
        if choice == "1":
            subscribe_user()
        elif choice == "2":
            stream_messages()
        elif choice == "3":
            break
        else:
            print("Invalid choice. Please enter a valid option.")

if __name__ == "__main__":
    print("-------------------------Organization alerts system-----------------------------")
    main()
