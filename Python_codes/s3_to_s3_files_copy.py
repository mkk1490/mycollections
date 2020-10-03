import boto
import boto.s3
import subprocess

bucket_name = "s3_bucket"
src_file_location = "file_location"
src_s3_path = "s3://src_bucket"
tgt_s3_path = "s3://tgt_bucket"
tgt_file_path = "/directory"

conn = boto.s3.connect_to_region('us-east-1')
bucket = conn.get_bucket(bucket_name)

# to skip a folder level --> "/", "/"

src_file_folders = bucket.list(src_file_location + "/", "/")

for folder in src_file_folders:
    source_s3_path = src_s3_path + folder.name + 'directory/'
    tgt_file_location = tgt_s3_path + tgt_file_path
copy_bashCommand = "aws s3 cp source_s3_path tgt_file_location --recursive"

print(copy_bashCommand)

copy_output = subprocess.check_output(['bash', '-c', copy_bashCommand])
