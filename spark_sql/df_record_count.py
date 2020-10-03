"""
Random scenarios:
How many entries in the dataset have an col_value greater than or equal to 100 AND a type of 1 or 2?
df can either be a file or a table
This example is using a df and

"""


df.where("col_value>=100").where("type==1 or type==2").count()

### count

df.count()
print("Count is : ", df.count())
