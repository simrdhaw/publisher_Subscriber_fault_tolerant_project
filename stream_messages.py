import argparse
import requests

broker_node = '127.0.0.1:5052' 

def stream_messages(username, broker_node):
    try:
        response = requests.get(f"{broker_node}/stream?username={username}", stream=True)
        if response.status_code == 200:
            print("Streaming messages from the master broker: ",broker_node)
            for line in response.iter_lines():
                if line:
                    print(line.decode('utf-8'))
    except requests.RequestException:
        if broker_node == '127.0.0.1:5052': 
            broker_node = '127.0.0.1:5053' 
        else:
            broker_node == '127.0.0.1:5052'
        print("Primary broker is down ,updating the broker the primary broker to "+broker_node)
        stream_messages(username, broker_node)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=True, help='Username to stream messages')
    parser.add_argument('--broker_node', dest='broker_node', required=True, help='Base URL of the server')
    args = parser.parse_args()

    stream_messages(args.username,args.BASE_URL)

