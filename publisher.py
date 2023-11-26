import requests
import json
import boto3


def get_running_aws_instances():
    # Create an EC2 client
    ec2 = boto3.client('ec2', region_name='us-east-2',aws_access_key_id='####', 
                aws_secret_access_key='######')  # Ensure your AWS credentials are configured
    # Retrieve information about running instances
    response = ec2.describe_instances()
    public_ips = []
    # Extract and display public IPv4 addresses of running instances
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            if 'PublicIpAddress' in instance:
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
    response = requests.get(f"http://{public_ips[1]}/list_replicas")
    response_data = response.json()  # Extract JSON data from the response

    # Accessing the 'replicas' key from the JSON data
    replicas = response_data.get('replicas', [])

    # Convert JSON strings to Python dictionaries
    replicas_data = [json.loads(replica) for replica in replicas]

    # Find the broker with is_primary=True
    primary_broker = next((json.loads(broker) for broker in replicas if json.loads(broker).get('is_primary', False)), None)

    if primary_broker:
        return primary_broker['ip']
        
    else:
        print("No primary broker found..")


broker_node = find_primary_broker() 



#broker_node = ''

def publish_topic():
    global broker_node
    print("Publishing messages to broker with ip: "+broker_node)
    topic = input("Enter topic: ")
    data = input("Enter data: ")
    try:
        response = requests.post(f"http://{broker_node}/publish?topic={topic}&data={data}")
        print(response.json())
    except requests.RequestException:
        active_nodes = get_running_aws_instances()
        broker_node = active_nodes[0]
        # if broker_node == '18.217.141.92:5053': 
        #     broker_node = '13.58.245.117:5052' 
            
        # else:
        #     broker_node == '18.217.141.92:5053'
        print("Primary broker is down ,updating the broker the primary broker to "+broker_node)
        publish_topic()
        

def main():
    print("Connecting to the primary broker....")
    print("Primary broker found with ip: ",broker_node)
    while True:
        print("\nMenu:")
        print("1. Publish Topic")
        print("2. Exit")

        choice = input("Enter your choice: ")

        if choice == "1":
            publish_topic()
        elif choice == "2":
            break
        else:
            print("Invalid choice. Please enter a valid option.")

if __name__ == "__main__":
    print("-------------------------Organization alerts system-----------------------------")
    main()
