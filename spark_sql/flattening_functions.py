from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window as W

# pivot- this can be any number
pvt = '10'
pivot_list = [str(item) for item in list(map(str, range(1, pvt + 1)))]

# create a list of values from a column
df = spark.read.table("schema_name.table_name")
list = [str(row.col_name) for row in df.collect()]

# add a prefix or suffix to pivoted columns

""" this will give an output as following for pivot number as 10:
Prefix_1, Prefix_2,..., Prefix_10
"""
# this is required for a single column from source pivoted to 10 different columns. If this is not used, then the column names will be only be numbers rangin from 1-10 for pvt =10
cols = df.columns
col = ['Prefix' + x for x in cols]

prefix = 'col'
cols = df.columns
col = list(map(lambda x: prefix + ''.join(x for x in x.split('_')[::-1]), cols))


""" this will give an output as following for pivot number as 10:
Prefix_01, Prefix_02,..., Prefix_10

zfill(2) will add 0 until a max of 2 numbers is reached.
if you need to pivot 100 columns and need columns named as 001,002, 011,012,.. ,099,100, then use zfill(3)

"""

list = [str(item).zfill(2) for item in list(map(str, range(1, pvt + 1)))]