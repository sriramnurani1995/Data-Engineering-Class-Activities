from google.cloud import pubsub_v1

# GCP Configuration
project_id = 'soy-blueprint-420318'
subscription_id = 'busbreadcrumbdata-sub'  

# Subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def acknowledge_until_cleared(subscriber, subscription_path):
    while True:
        # Construct the pull request
        request = {"subscription": subscription_path, "max_messages": 100}  

        # Pull messages
        response = subscriber.pull(request=request)

        received_messages = response.received_messages

        if not received_messages:
            break 

        # Extract acknowledgment IDs
        ack_ids = [message.ack_id for message in received_messages]

        # Acknowledge messages
        subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})

def main():
    acknowledge_until_cleared(subscriber, subscription_path)
    print("All messages in the topic have been acknowledged and cleared.")

if __name__ == "__main__":
    main()
