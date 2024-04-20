import argparse
from google.cloud import pubsub_v1
import json
import os

# GCP Configuration
project_id = 'soy-blueprint-420318'

# File to store the records
file_path = 'pubsub_records.json'

def callback(message, file_path):
    print(f"Received message: {message.data}")
    try:
        # Decode the message data from bytes to a string and parse into JSON
        data = json.loads(message.data.decode('utf-8'))
        
        # Append the data to the file
        with open(file_path, 'a') as file:
            file.write(json.dumps(data) + '\n')

        # Acknowledge the message so it won't be sent again
        message.ack()
    except Exception as e:
        print(f"An error occurred: {e}")

def main(subscription_id):
    # Subscriber client
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Open the file in append mode, or create it if it doesn't exist
    if not os.path.exists(file_path):
        with open(file_path, 'w'):
            pass

    # Custom callback that also sends file_path
    custom_callback = lambda message: callback(message, file_path)

    # Listen for messages on the subscription
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=custom_callback)
    print(f"Listening for messages on {subscription_path}...")

    # Keep the main thread alive, or the subscriber will stop listening
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Subscribe to a Pub/Sub subscription and log messages.")
    parser.add_argument("subscription_id", type=str, help="The subscription ID to subscribe to.")
    args = parser.parse_args()

    main(args.subscription_id)
