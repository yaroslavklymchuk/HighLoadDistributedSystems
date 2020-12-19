import json
import time

from pyspark.sql import SparkSession

from processing import TaxiDataProcessor


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option") \
    .getOrCreate()
sc = spark.sparkContext
data_file = "data.txt"
df = sc.textFile(data_file, minPartitions=100).map(lambda x: eval(x))
td = TaxiDataProcessor(df)

'''
top_drivers = td.top_drivers(100)
with open("results/top_drivers.json", "w") as f:
    json.dump(top_drivers, f, indent=4, sort_keys=True)

bad_drivers = td.drivers_rating_lower_than(3.5)
with open("results/bad_drivers.json", "w") as f:
   json.dump(bad_drivers, f, indent=4, sort_keys=True)

n = 0
result_dict = {}
for hour, rides in td.most_intensive_timeframe():
   result_dict[n] = {}
   result_dict[n]['start'] = hour
   result_dict[n]['end'] = hour + 1
   result_dict[n]['qty_rides'] = rides
   n += 1
with open("results/timeframes.json", "w") as f:
   json.dump(result_dict, f, indent=4, sort_keys=True)

top_clients = td.top_clients(50)
with open("results/top_clients.json", "w") as f:
   json.dump(top_clients, f, indent=4, sort_keys=True)

count_drivers = td.count_drivers()
with open("results/count_drivers.json", "w") as f:
   json.dump(count_drivers, f, indent=4, sort_keys=True)
    
count_clients = td.count_clients()
with open("results/count_clients.json", "w") as f:
   json.dump(count_clients, f, indent=4, sort_keys=True)

top_earners = td.top_earners(100)
with open("results/top_earners.json", "w") as f:
   json.dump(top_earners, f, indent=4, sort_keys=True)

top_night_drivers = td.top_nightwolves(50)
with open("results/top_night_drivers.json", "w") as f:
   json.dump(top_night_drivers, f, indent=4, sort_keys=True)

top_praised_drivers = td.most_praised_driver_quality()
with open("results/top_praised_drivers.json", "w") as f:
   json.dump(top_praised_drivers, f, indent=4, sort_keys=True)

top_complained_drivers = td.most_complained_driver_quality()
with open("results/top_complained_drivers.json", "w") as f:
   json.dump(top_complained_drivers, f, indent=4, sort_keys=True)
'''
most_len_comment = td.driver_comment()
with open("results/most_len_comment.json", "w") as f:
    json.dump(most_len_comment, f, indent=4, sort_keys=True)
