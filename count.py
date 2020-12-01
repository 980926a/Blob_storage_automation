from azure.storage.blob import BlobServiceClient
import os

connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
container = "smartwatchdata"
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container)
blobs_list = container_client.list_blobs()
lst_blobs_list = list(blobs_list)
num = 0
size = 0
for blob in lst_blobs_list:
    num += 1  # num = num + 1
    if len(lst_blobs_list) == num:
        print("Count: ", num)
    else:
        continue
