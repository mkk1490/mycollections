import spark as spark

s3_bucket_name = "bucket_name"  # without 's3://'

df_read = spark.read.table("schema_name.table_name")

df_pandas = df_read.coalesce(1).toPandas()

from io import StringIO  # python3; python2: BytesIO
import boto3

csv_buffer = StringIO()
s3_resource = boto3.resource('s3')

df_pandas.to_csv(csv_buffer)

s3_resource = boto3.resource('s3')
s3_resource.Object('s3_bucket_name', '{8folder_level_1}/{folder_level_2}/{folder_level_3}/{file_name}.csv').put(
    Body=csv_buffer.getvalue())
