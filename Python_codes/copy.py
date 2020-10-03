import shutil
from typing import Text, List
from pyspark.sql import SparkSession, DataFrame
import boto3

from ..file_operations.util import generate_comma_seperated_filenames_from_keylist, resolve_s3_prefix, \
    execute_shell_command, create_db_connection, validate_s3_path


def copy_s3_to_local(bucket: Text, destination_path: Text, source_file_to_download: Text) -> None:
    from botocore.exceptions import ClientError
    """
        Downloads the files from AWS S3 to EMR NFS location
    Raises:
        botocore.exceptions.ClientError: The exception is thrown when the file is not found in S3            

    """
    if len(source_file_to_download) <= 0:
        raise NoFilesToProcessError
    input_file = generate_comma_seperated_filenames_from_keylist(source_file_to_download=source_file_to_download)
    src_path = "{}{}{}".format(resolve_s3_prefix(source_file_to_download=source_file_to_download), "/", input_file)
    if destination_path.endswith("/"):
        local_path = "{}{}".format(destination_path, input_file)
    else:
        local_path = "{}/{}".format(destination_path, input_file)
    s3 = boto3.client('s3')
    try:
        with open(local_path, 'wb') as data:
            s3.download_fileobj(bucket, src_path, data)
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


def clean_up_files_hdfs(*, path: Text, **kwargs) -> None:
    """
        cleans up the hdfs after parquet processing

    """
    if path.endswith("/"):
        cleanup_path = "{}*".format(path)
    else:
        cleanup_path = "{}/*".format(path)
    hdfs_command = ['hdfs', 'dfs', '-rm', '-R', cleanup_path]
    command = ' '.join(hdfs_command)
    execute_shell_command(shell_command=command)


def clean_up_files_dbfs(*, path: Text, **kwargs) -> None:
    """
        cleans up the dbfs after parquet processing

    """
    command = ['rm', '-f', "{}*.*".format(path)]
    command = ' '.join(command)
    execute_shell_command(shell_command=command)


def copy_local_to_hdfs(source_nfs_path: Text, destination_hdfs_path: Text = "/tmp/"):
    """
        copies files from local to HDFS

    """
    nfs_dir = "{}/*".format(source_nfs_path)
    hdfs_command = ['hdfs', 'dfs', '-put', '-f', nfs_dir, destination_hdfs_path]
    command = ' '.join(hdfs_command)
    execute_shell_command(shell_command=command)


def copy_local_to_dbfs(source_nfs_path: Text, dbutils, destination_hdfs_path: Text = "/tmp/"):
    """
        copies files from local to DBFS

    """

    dbutils.fs.cp("file:{}".format(source_nfs_path), "dbfs:{}".format(destination_hdfs_path), recurse=True)


def copy_local_to_s3() -> None:
    # TODO
    """
        uploads the files from EMR NFS location to AWS S3

    Args:
        
    """


def clean_up_files(*, destination_path: Text, **kwargs) -> None:
    """
        cleans up the original source files after unzip and untar

    """
    shutil.rmtree(destination_path)


def s3_to_redshift_parquet(dbname: Text, port: Text, user: Text, password: Text,
                                host: Text, s3_uri: Text, schema: Text, table_name: Text,
                                redhift_role_arn: Text, mode: Text, **kwargs):
    copy_command = ["copy", "{}.{}".format(schema, table_name), "from", "'{}'".format(s3_uri),
                    "credentials", "'aws_iam_role={}'".format(redhift_role_arn), "format", "parquet", ";", "commit;"]
    print(" ".join(copy_command))
    con = create_db_connection(dbname=dbname, port=port, user=user, password=password, host=host)
    cur = con.cursor()
    try:
        if mode == "overwrite":
            truncate_command = ["truncate", "table", "{}.{}".format(schema, table_name), ";", "commit;"]
            cur.execute(query=" ".join(truncate_command))
            cur.execute(query=" ".join(copy_command))
        else:
            cur.execute(query=" ".join(copy_command))
    except Exception as e:
        print(e)
        print("redshift copy process failed")
    finally:
        con.close()


def df_to_redshift_parquet(dbname: Text, port: Text, user: Text,
                                password: Text, host: Text, df: DataFrame, schema: Text,
                                table_name: Text, mode: Text, **kwargs):
    if df is not None:
        if mode is not None and mode.lower() == "overwrite":
            df.write.format("jdbc")\
                .option("driver", "org.postgresql.Driver") \
                .option("url", "jdbc:postgresql://{}:{}/{}".format(host, port, dbname)) \
                .option("user", user) \
                .option("password", password) \
                .option("dbtable", "{}.{}".format(schema, table_name)) \
                .mode("overwrite").save()
        elif mode is not None and mode.lower() == "append":
            df.write.format("jdbc")\
                .option("driver", "org.postgresql.Driver") \
                .option("url", "jdbc:postgresql://{}:{}/{}".format(host, port, dbname)) \
                .option("user", user) \
                .option("password", password) \
                .option("dbtable", "{}.{}".format(schema, table_name)) \
                .mode("append").save()
        else:
            raise ModeNotDefinedProcessError
    else:
        raise EmptyDataFrameError


def redshift_to_s3_parquet(dbname: Text, port: Text, user: Text, password: Text,
                                host: Text, s3_uri: Text, query: Text,
                                redhift_role_arn: Text, **kwargs):
    copy_command = ["unload",  "('{}')".format(query), "to", "'{}'".format(s3_uri),
                    "credentials", "'aws_iam_role={}'".format(redhift_role_arn), "format", "parquet"]
    print(" ".join(copy_command))
    con = create_db_connection(dbname=dbname, port=port, user=user, password=password, host=host)
    cur = con.cursor()
    try:
        copy_command = ["unload", "('{}')".format(query), "to", "'{}'".format(s3_uri),
                        "credentials", "'aws_iam_role={}'".format(redhift_role_arn),
                        "format", "parquet", "ALLOWOVERWRITE"]
        cur.execute(query=" ".join(copy_command))
    except Exception as e:
        print(e)
        print("redshift copy process failed")
    finally:
        con.close()


def redshift_command_executor(dbname: Text, port: Text, user: Text, password: Text,
                                host: Text, redshift_command: Text, **kwargs):
    con = create_db_connection(dbname=dbname, port=port, user=user, password=password, host=host)
    cur = con.cursor()
    try:
        cur.execute(query=redshift_command)
    except Exception as e:
        print(e)
        print("redshift copy process failed")
    finally:
        con.close()


def redshift_to_df_parquet(spark: SparkSession, dbname: Text, port: Text, user: Text, password: Text,
                                host: Text, query: Text = None, **kwargs) -> DataFrame:
    dbtable = kwargs.get("dbtable", None)
    partition_column = kwargs.get("partition_column", None)
    lower_bound = kwargs.get("lower_bound", None)
    upper_bound = kwargs.get("upper_bound", None)
    num_partitions = kwargs.get("num_partitions", None)
    if all(v is not None for v in
           [dbtable, partition_column, lower_bound, upper_bound, num_partitions]) and query is None:
        df = spark.read.format("jdbc") \
            .option("url",
                    "jdbc:postgresql://{}:{}/{}".format(host, port, dbname)) \
            .option("dbtable", dbtable) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .option("numPartitions", num_partitions) \
            .option("partitionColumn", partition_column) \
            .option("lowerBound", lower_bound) \
            .option("upperBound", upper_bound) \
            .load()
        return df
    else:
        df = spark.read.format("jdbc") \
            .option("url",
                    "jdbc:postgresql://{}:{}/{}".format(host, port, dbname)) \
            .option("query", query) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df


def sqlserver_to_df_parquet(spark: SparkSession, dbname: Text, port: Text, user: Text, password: Text,
                                host: Text, query: Text = None, **kwargs) -> DataFrame:
    dbtable = kwargs.get("dbtable", None)
    partition_column = kwargs.get("partition_column", None)
    lower_bound = kwargs.get("lower_bound", None)
    upper_bound = kwargs.get("upper_bound", None)
    num_partitions = kwargs.get("num_partitions", None)
    if all(v is not None for v in
           [dbtable, partition_column, lower_bound, upper_bound, num_partitions]) and query is None:
        df = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
            .option("url",
                    "jdbc:sqlserver://{}:{};databaseName={}".format(host, port, dbname)) \
            .option("dbtable", dbtable) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("numPartitions", num_partitions) \
            .option("partitionColumn", partition_column) \
            .option("lowerBound", lower_bound) \
            .option("upperBound", upper_bound) \
            .load()
        return df
    else:
        df = spark.read.format("jdbc") \
            .option("url",
                    "jdbc:sqlserver://{}:{};databaseName={}".format(host, port, dbname)) \
            .option("query", query) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .load()
        return df


def df_to_sqlserver_parquet(dbname: Text, port: Text, user: Text,
                            password: Text, host: Text, df: DataFrame, schema: Text,
                            table_name: Text, mode: Text, **kwargs):
    if df is not None:
        if mode is not None and mode.lower() == "overwrite":
            df.write.format("com.microsoft.sqlserver.jdbc.spark")\
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .option("url", "jdbc:sqlserver://{}:{};databaseName={}".format(host, port, dbname)) \
                .option("user", user) \
                .option("password", password) \
                .option("dbtable", "{}.{}".format(schema, table_name)) \
                .option("truncate", "true") \
                .mode("overwrite").save()
        elif mode is not None and mode.lower() == "append":
            df.write.format("jdbc")\
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .option("url", "jdbc:sqlserver://{}:{};databaseName={}".format(host, port, dbname)) \
                .option("user", user) \
                .option("password", password) \
                .option("dbtable", "{}.{}".format(schema, table_name)) \
                .mode("append").save()
        else:
            raise ModeNotDefinedProcessError
    else:
        raise EmptyDataFrameError
