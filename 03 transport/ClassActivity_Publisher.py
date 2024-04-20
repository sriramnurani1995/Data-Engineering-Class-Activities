import urllib.request
import json
from google.cloud import pubsub_v1


# GCP Configuration
project_id = 'soy-blueprint-420318'
topic_id = 'busbreadcrumbdata'

# Publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# List of vehicle IDs
vehicle_ids = [3029, 3235]

def fetch_data(vehicle_id):
    """Fetch JSON data for a vehicle."""
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    try:
        with urllib.request.urlopen(url) as response:
            return json.loads(response.read().decode())
    except urllib.error.URLError as e:
        print(f"Failed to fetch data for vehicle ID {vehicle_id}: {e}")
        return None

def save_to_file(all_data, filename='bcsample.json'):
    """Save all data to a JSON file."""
    with open(filename, 'w') as file:
        json.dump(all_data, file, indent=4)
    print(f"Data saved to {filename}")

def publish_to_pubsub(filename='bcsample.json'):
    """Publish contents of JSON file to Pub/Sub."""
    with open(filename, 'r') as file:
        records = json.load(file)
        for record in records:
            message_bytes = json.dumps(record).encode('utf-8')
            future = publisher.publish(topic_path, message_bytes)
            print(f"Message published. ID: {future.result()}")
            

def main():
    # all_data = []  # Initialize an empty list to hold all records

    # # Fetch and append data for each vehicle
    # for vehicle_id in vehicle_ids:
    #     data = fetch_data(vehicle_id)
    #     if data:
    #         all_data.extend(data)  # Append data to the list
    
    # # Save all data to a new file
    # save_to_file(all_data)

    # Publish to Pub/Sub
    publish_to_pubsub()

if __name__ == "__main__":
    main()
