from pyspark.sql import SparkSession
import sys
import time
from queries.STQuery import STQuery
from partitions.STPartition import STPartition

#choose partition
val = input("Choose partition: ")
STPartition(val.lower())

#starting spark session & load data
spark = SparkSession.builder.appName("bench-ranking").getOrCreate()
df = spark.read.load(f"./data/SingleStmtTable100K{val}.csv",
                     format="csv", sep=",", header="true", inferSchema="true")

df.createOrReplaceTempView("SingleStmtTable")

#start query
query = [STQuery().q1, STQuery().q2,STQuery().q3,STQuery().q4,STQuery().q5,STQuery().q6,STQuery().q7,STQuery().q8,STQuery().q9,STQuery().q10,STQuery().q11]
counter = 1
for i in query:
  start_time = time.time()
  df2 = spark.sql(i).show()
  timer = time.time() - start_time

  original_stdout = sys.stdout # Save a reference to the original standard output
  with open('log.txt', 'a') as f:
      sys.stdout = f # Change the standard output to the file we created.
      print("QUERY " + str(counter) + " = " + str(timer) + " ")
      sys.stdout = original_stdout # Reset the standard output to its original value
      counter += 1
      
print("all queries are done -csv -st")