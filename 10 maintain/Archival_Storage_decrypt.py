import json
import zlib
from google.cloud import storage
from datetime import datetime
import rsa
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend

# GCP Configuration
bucket_name = "testarchival"  # Replace with your bucket name
file_name = f"archive_{datetime.now().strftime('%Y-%m-%d')}.json.gz.enc"

# Initialize storage client
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(file_name)

# Load RSA keys from files
with open('public_key.pem', 'rb') as f:
    public_key = rsa.PublicKey.load_pkcs1(f.read())

with open('private_key.pem', 'rb') as f:
    private_key = rsa.PrivateKey.load_pkcs1(f.read())

# Load AES key from file (if you choose to use a fixed AES key, otherwise skip this part)
# with open('aes_key.bin', 'rb') as f:
#     aes_key = f.read()

def decrypt_data_with_aes(encrypted_data, key):
    """Decrypt data using AES."""
    iv = encrypted_data[:16]
    encrypted_data = encrypted_data[16:]

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    padded_data = decryptor.update(encrypted_data) + decryptor.finalize()

    unpadder = padding.PKCS7(128).unpadder()
    data = unpadder.update(padded_data) + unpadder.finalize()
    return data

def download_and_decrypt():
    """Download, decrypt, and decompress the data from GCS."""
    try:
        final_data = blob.download_as_bytes()
        encrypted_key = final_data[:256]
        encrypted_data = final_data[256:]

        # Decrypt the AES key with RSA
        aes_key = rsa.decrypt(encrypted_key, private_key)

        # Decrypt the data with AES
        compressed_data = decrypt_data_with_aes(encrypted_data, aes_key)
        decompressed_data = zlib.decompress(compressed_data)
        data = json.loads(decompressed_data.decode('utf-8'))

        # Save the decrypted data to a JSON file
        output_file_name = f"decrypted_data_{datetime.now().strftime('%Y-%m-%d')}.json"
        with open(output_file_name, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        
        print(f"Decrypted data saved to {output_file_name}")

    except Exception as e:
        print(f"An error occurred during decryption and decompression: {e}")

if __name__ == "__main__":
    download_and_decrypt()
