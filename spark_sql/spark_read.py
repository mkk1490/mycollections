import spark

############################################################ csv #######################################################

# delimiter can be anything based on the file. It can be ',', '|' or any custom delimiter

# if the delimiter consists of multiple characters, we need to handle that separately


df_csv = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("file_location/file")
df_csv.show(10)

############################################################ XML #######################################################
"""

get rowtag as input for xml files we can extract data for multiple tables from a single xml file. They will be 
differentiated by rowtags. Here's a snippet of xml code without the root tag: 

<Table1>
<col1>value</col1>
<col2>value</col2>
</Table1>
<Table2>
<col1>value</col1>
<col2>value</col2>
</Table2>

rowtag = 'Table1'
rowtag = 'Table2

Need to be given in separate read statements


"""

df_xml = spark.read.format("com.databricks.spark.xml").option("rowTag", rowtag).load("file_location/file")
df_xml.registerTempTable("df_xml")

############################################################ parquet #######################################################

df_parquet = spark.read.parquet("/file_location/file")
df_parquet.createOrReplaceTempView("df_parquet")



