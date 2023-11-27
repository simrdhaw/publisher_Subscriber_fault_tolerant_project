import requests
import subprocess
import platform
import sys
import boto3
import json

def get_running_aws_instances():
    # Create an EC2 client
    ec2 = boto3.client('ec2', region_name='us-east-2',aws_access_key_id='###', 
                aws_secret_access_key='####')  # Ensure your AWS credentials are configured
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

def list_replicas():
    broker_node = input("List replicas at?? :")
    response = requests.get(f"http://{broker_node}/list_replicas")
    print(type(response))
    print(response.json())

def add_replica():
    broker_node = input("Enter the node to which you want to add the replica: ")
    broker_id = input("Enter broker ID: ")
    broker_ip = input("Enter broker IP: ")
    is_primary = input("Is it primary? (1/0): ")
    #There should be some input to which server this replica has to be added
    response = requests.post(f"http://{broker_node}/replicate/add_replica?broker_id={broker_id}&broker_ip={broker_ip}&is_primary={is_primary}")
    print(response.json())



def initialize_broker():
    broker_node = input("Enter the server id where you want initialise the broker: ")
    broker_id = input("Enter broker ID: ")
    broker_ip = input("Enter broker IP: ")
    is_primary = input("Is it primary? (1/0): ")
    response = requests.post(f"http://{broker_node}/initialize_broker?broker_id={broker_id}&broker_ip={broker_ip}&is_primary={is_primary}")
    print(response.json())


    

def main():
    public_ips = get_running_aws_instances()
    print("List of instances running in aws:",public_ips)
    while True:
        print("\nMenu:")
        print("1. List Brokers")
        print("2. Add Replica")
        print("3. Initialize Broker")
        
        print("4. Exit")
        choice = input("Enter your choice: ")
        if choice == "1":
            list_replicas()
        elif choice == "2":
            add_replica()
        elif choice == "3":
            initialize_broker()
        elif choice == "4":
            break
        else:
            print("Invalid choice. Please enter a valid option.")

if __name__ == "__main__":
    print("-------------------------Organization alerts system-----------------------------")
    print("Set up the brokers")
    main()
