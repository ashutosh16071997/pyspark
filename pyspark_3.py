#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from pyspark.sql import SparkSession
import sys,os
import csv
if len(sys.argv) != 3:
	print("Incorrect number of arguments")
	sys.exit(0)
cores = sys.argv[1]
if(int(cores) <= 0):
	print("Partitions must be greater that 2")
	sys.exit(0)
spark = SparkSession.builder.appName("groupbyagg").getOrCreate()	
input_file = "airports.csv"
output_file = sys.argv[2]
df = spark.read.csv(input_file, inferSchema=True, header=True)
#------------------------------------------------------------------------------------------------
new_df=df.repartition(int(cores)) # SETTING NUMBER OF CORES
#os.environ["SPARK_WORKER_CORES"] = cores
new_df.write.csv("Partitions_3")
#------------------------------------------------------------------------------------------------
print("Number of Partitions: ",new_df.rdd.getNumPartitions()) #SHOWING NUMBER OF PARTITIONS
#------------------------------------------------------------------------------------------------
print("Partitioner: {}".format(new_df.rdd.partitioner)) # TYPE OF PARTITION APPLIED
#------------------------------------------------------------------------------------------------

x=new_df.filter((new_df["LATITUDE"] >= 10) & (new_df["LATITUDE"] <= 90) & (new_df["LONGITUDE"] >= -90) & (new_df["LONGITUDE"] <= -10))
output_file1 = output_file
name1 = output_file.split(".")[0]
res = x.toPandas()["NAME"]
x.toPandas()["NAME"].to_csv(output_file1,index = False,header = True)
#f = open(output_file, "w")
#f.write(str(res))
#f.close()

#with open(output_file,"w") as outputfile:
#	with open(name1,"r") as tot:
#		[outputfile.write(" ".join(row)+'\n') for row in csv.reader(tot)]
#outputfile.close()
#tot.close()
