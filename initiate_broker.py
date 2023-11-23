import requests
import subprocess
import platform
import sys



def list_replicas():
    broker_node = input("Enter the node for which you want to display it's replicas")
    response = requests.get(f"http://{broker_node}/list_replicas")
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
