#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# =============================================================================
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("groupbyagg").getOrCreate()
# df = spark.read.csv('airports.csv', inferSchema=True, header=True)
# x=df.groupBy('COUNTRY').count()
# x.coalesce(1).write.csv("result.csv")
# =============================================================================

from pyspark.sql import SparkSession
import sys,os
import csv
if len(sys.argv) != 3:
	print("Incorrect number of arguments")
	sys.exit(0)
spark = SparkSession.builder.appName("groupbyagg").getOrCreate()
cores = sys.argv[1]
if(int(cores) <= 0):
	print("Partitions must be greater that 0")
	sys.exit(0)
input_file = "airports.csv"
output_file = sys.argv[2]
df = spark.read.csv(input_file, inferSchema=True, header=True) #reading data
#------------------------------------------------------------------------------------------------
new_df=df.repartition(int(cores)) # SETTING NUMBER OF CORES
#os.environ["SPARK_WORKER_CORES"] = cores
#------------------------------------------------------------------------------------------------
print("Number of partitions: ",new_df.rdd.getNumPartitions()) #SHOWING NUMBER OF PARTITIONS
new_df.write.csv("Partitions_1")
#------------------------------------------------------------------------------------------------
print("Partitioner: {}".format(new_df.rdd.partitioner)) # TYPE OF PARTITION APPLIED
#------------------------------------------------------------------------------------------------
x=new_df.groupBy('COUNTRY').count()
#x.coalesce(1).write.csv(output_file+".csv")
output_file1 = output_file
name1 = output_file.split(".")[0]
x.toPandas().to_csv(output_file1,index = False,header = True)

#with open(output_file,"w") as outputfile:
#	with open(name1,"r") as tot:
#		[outputfile.write(" ".join(row)+'\n') for row in csv.reader(tot)]
#outputfile.close()
#tot.close()		
