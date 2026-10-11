
import boto3
from botocore.exceptions import ClientError

bucket_name = "emr--test-bucket"
local_file = "carts.json"
s3_key = "raw/carts.json"

try:
    s3 = boto3.client("s3", region_name="ap-south-1")

    s3.upload_file(local_file, bucket_name, s3_key)

    print("File uploaded successfully to S3!")

except FileNotFoundError:
    print("Error: carts.json was not found.")

except ClientError as e:
    print("S3 upload failed:", e)
