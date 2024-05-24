import json
from google.cloud import pubsub_v1, storage
from datetime import datetime, timedelta
import threading
from threading import Lock
import time

# GCP Configuration
project_id = 'focus-surfer-420318'
subscription_id = 'archivetest-sub'
bucket_name = 'testarchival' # Replace with your bucket name

# Timeout configuration
timeout_minutes = 3  
last_message_time = datetime.now()
# Global lock for synchronizing access to batch_data
batch_data_lock = Lock()
batch_data = []
batch_size = 10000  # Increased batch size to 10,000

# Initialize storage client
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
today = datetime.now().strftime("%Y-%m-%d")
file_name = f"archive_{today}.json"
blob = bucket.blob(file_name)

class PersistentTimer:
    def __init__(self, action, check_interval=60, timeout_minutes=3):
        self.timeout_minutes = timeout_minutes
        self.action = action
        self.check_interval = check_interval
        self.timer = threading.Timer(self.check_interval, self.run)
        self._lock = threading.Lock()

    def run(self):
        with self._lock:
            global last_message_time
            
            if datetime.now() - last_message_time >= timedelta(minutes=self.timeout_minutes):
                self.action()
            self.timer = threading.Timer(self.check_interval, self.run)
            self.timer.start()

    def start(self):
        with self._lock:
            self.timer.start()

    def stop(self):
        with self._lock:
            if self.timer is not None:
                self.timer.cancel()
                self.timer = None

def upload_data_to_gcs(data):
    """Append data to the JSON file in GCS."""
    retry_count = 0
    max_retries = 5
    while retry_count < max_retries:
        try:
            existing_data = ""
            if blob.exists():
                existing_data = blob.download_as_text()
            existing_json = json.loads(existing_data) if existing_data else []
            existing_json.extend(data)
            updated_data_string = json.dumps(existing_json)
            blob.upload_from_string(updated_data_string, content_type='application/json')
            print(f"Uploaded {len(data)} messages to {bucket_name}/{file_name}")
            break  # Exit loop if upload is successful
        except Exception as e:
            retry_count += 1
            wait_time = min(2 ** retry_count, 30)  # Exponential backoff with cap
            print(f"An error occurred during upload (attempt {retry_count}): {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    else:
        print(f"Failed to upload after {max_retries} retries.")

def cancel_subscription():
    global batch_data, last_message_time
    last_message_time = datetime.now()
    print("Timeout reached. Processing and uploading messages to GCS...")
    streaming_pull_future.cancel()
    
    with batch_data_lock:
        if batch_data:
            upload_data_to_gcs(batch_data)
            batch_data = []  

def callback(message):
    global last_message_time, batch_data
    try:
        data = json.loads(message.data.decode('utf-8'))
        
        with batch_data_lock:
            batch_data.append(data)
            if len(batch_data) >= batch_size:
                upload_data_to_gcs(batch_data)
                batch_data = []  # Reset the batch data after processing

        message.ack()
        last_message_time = datetime.now()

    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    global streaming_pull_future, subscriber
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    timer = PersistentTimer(cancel_subscription)
    timer.start()

    while True:
        try:
            streaming_pull_future = subscriber.subscribe(subscription_path, callback)
            print(f"Listening for messages on {subscription_path}...")
            streaming_pull_future.result()
        except KeyboardInterrupt:
            print("Process interrupted by user.")
            break
        except TimeoutError:
            print("Timeout reached with no messages.")
        finally:
            streaming_pull_future.cancel()
            timer.stop()
            print("Subscriber and timer stopped.")
        print("Attempting to reconnect after timeout or error...")

    subscriber.close()
    print("Subscriber closed.")

if __name__ == "__main__":
    main()
