import requests
import subprocess
import platform
import sys

broker_node = '127.0.0.1:5052' 

def subscribe_user():
    global broker_node
    
    username = input("Enter username: ")
    topic = input("Enter topic to subscribe: ")
    try:
        response = requests.post(f"http://{broker_node}/subscribe?username={username}&topic={topic}")
        print(response.json())
    except requests.RequestException:
        
        if broker_node == '127.0.0.1:5052': 
            broker_node = '127.0.0.1:5053' 
            
        else:
            broker_node == '127.0.0.1:5052'
        print("Primary broker is down ,updating the broker the primary broker to "+broker_node)
        
        subscribe_user()
        

def stream_messages():
    username = input("Enter username to stream messages: ")
    if sys.platform.startswith('win'):  # For Windows
        subprocess.Popen(['start', 'cmd', '/k', 'python', 'stream_messages.py', '--username', username,'--broker_node',broker_node], shell=True)
    elif sys.platform == 'darwin':  # For macOS
        subprocess.Popen(['open', '-a', 'Terminal', 'python', 'stream_messages.py', '--username', username,'--broker_node',broker_node])
    elif sys.platform.startswith('linux'):  # For Linux
        subprocess.Popen(['x-terminal-emulator', '-e', 'python', 'stream_messages.py', '--username', username,'--broker_node',broker_node])
    else:
        print("Unsupported platform")

def main():
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


def my_stream():
    global resp
    resp = requests.get("http://127.0.0.1:5000/stream", params={'username': 'simran'}, stream=True)
    for d in resp.content:
        print(d)

if __name__ == "__main__":
    print("-------------------------Organization alerts system-----------------------------")
    main()
