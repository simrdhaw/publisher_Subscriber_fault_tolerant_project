import requests
import json

# def get_updated_base_url():
#     with open('master_broker.txt', 'r') as file:
#         return file.readline().strip()


broker_node = '127.0.0.1:5052' 

def publish_topic():
    global broker_node
    print("Publishing messages to broker with ip: "+broker_node)
    topic = input("Enter topic: ")
    data = input("Enter data: ")
    try:
        response = requests.post(f"http://{broker_node}/publish?topic={topic}&data={data}")
        print(response.json())
    except requests.RequestException:
        if broker_node == '127.0.0.1:5052': 
            broker_node = '127.0.0.1:5053' 
        else:
            broker_node == '127.0.0.1:5052'
        print("Primary broker is down ,updating the broker the primary broker to "+broker_node)
        publish_topic()
        




def main():
    
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
