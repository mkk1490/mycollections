def run(*args, spark, **kwargs):
    table_list = ['table_1', 'table_2']

    for t in table_list:
        spark.sql(
            """alter table schema_name.{table_name} drop partition(partition_name = 'value')""".format(
                table_name=t))


run(spark=spark)


