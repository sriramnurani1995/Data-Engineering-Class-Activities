from google.cloud import pubsub_v1
import json
import os

# GCP Configuration
project_id = 'soy-blueprint-420318'
subscription_id = 'busbreadcrumbdata-sub'  # Replace with your subscription ID

# Subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# File to store the records
file_path = 'pubsub_records1.json'

def callback(message):
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

def main():
    # Open the file in append mode, or create it if it doesn't exist
    if not os.path.exists(file_path):
        with open(file_path, 'w'):
            pass

    # Listen for messages on the subscription
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    # Keep the main thread alive, or the subscriber will stop listening
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == "__main__":
    main()
