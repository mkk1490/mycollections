import spark

table_list = ['table_1', 'table_2']
for t in table_list:
    df = spark.sql("""desc formatted schema_name.{table_name}""".format(
        table_name=t))
    print(df.filter("col_name=='Location'").collect()[0].data_type)

# for one table

# spark.sql("""desc formatted schema_name.table_name""").filter("col_name=='Location'").collect()[0].data_type
