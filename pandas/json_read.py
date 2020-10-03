"""

sampleJson = [
 ('{"user":100, "ips" : ["191.168.192.101", "191.168.192.103", "191.168.192.96", "191.168.192.99"]}',),
 ('{"user":101, "ips" : ["191.168.192.102", "191.168.192.105", "191.168.192.103", "191.168.192.107"]}',),
 ('{"user":102, "ips" : ["191.168.192.105", "191.168.192.101", "191.168.192.105", "191.168.192.107"]}',),
 ('{"user":103, "ips" : ["191.168.192.96", "191.168.192.100", "191.168.192.107", "191.168.192.101"]}',),
 ('{"user":104, "ips" : ["191.168.192.99", "191.168.192.99", "191.168.192.102", "191.168.192.99"]}',),
 ('{"user":105, "ips" : ["191.168.192.99", "191.168.192.99", "191.168.192.100", "191.168.192.96"]}',),
]

Expected output:
ip	          count
191.168.192.96	3
191.168.192.99	6
191.168.192.100	2
191.168.192.101	3
191.168.192.102	2
191.168.192.103	2
191.168.192.105	3
191.168.192.107	3

"""


import json
import pandas as pd
sampl = []
for i in sampleJson:
    data = json.loads(i[0])
    sampl.append(data)
usr_data = pd.DataFrame(sampl)

samplejson = spark.createDataFrame(usr_data)
samplejson.registerTempTable("samplejson")
ips_count = spark.sql("select trim(ip) as ips, count(trim(ip)) as count from samplejson lateral view explode("
                      "ips)iplist as ip group by 1")

display(ips_count)