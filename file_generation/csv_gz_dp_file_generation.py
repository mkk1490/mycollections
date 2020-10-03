import spark

s3_path = "s3://bucket_name"


df = spark.read.table("schema_name.table_name")

df.write.mode("overwrite").option("header", "true").csv("s3_path/location/", compression="gzip")