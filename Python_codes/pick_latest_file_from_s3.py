import boto3

s3_client = boto3.client('s3')
response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=src_loc)
all = response['Contents']
latest = dict()
latest = max(all, key=lambda x: x['LastModified'])
file_name = latest['Key']
print("The latest file is here: " + file_name)




