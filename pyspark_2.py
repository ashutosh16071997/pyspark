#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from pyspark.sql import SparkSession
import sys,os
import csv
import numpy as np
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
df = spark.read.load(input_file,format="csv", inferSchema="true", header="true")
#------------------------------------------------------------------------------------------------
new_df=df.repartition(int(cores)) # SETTING NUMBER OF CORES
#os.environ["SPARK_WORKER_CORES"] = cores
new_df.write.csv("Partitions_2")
#------------------------------------------------------------------------------------------------
print("Number of partitions :",new_df.rdd.getNumPartitions()) #SHOWING NUMBER OF PARTITIONS
#------------------------------------------------------------------------------------------------
print("Partitioner: {}".format(new_df.rdd.partitioner)) # TYPE OF PARTITION APPLIED
#------------------------------------------------------------------------------------------------
	
x_grouped=new_df.groupby('COUNTRY').count()
#result=x_grouped.orderBy(x_grouped["count"].desc()).collect()
result=x_grouped.orderBy(x_grouped["count"].desc())
finaldf=result.limit(1)
#finaldf.toPandas().to_csv(output_file)
#np.savetxt(r'.\output_file', finaldf.values, fmt='%d')
output_file1 = output_file
name1 = output_file.split(".")[0]
res =  finaldf.first().COUNTRY
#res.toPandas().to_csv(output_file1,index = False)
f = open(output_file, "w")
f.write(res)
f.close()

#with open(output_file,"w") as outputfile:
#	with open(name1,"r") as tot:
#		[outputfile.write(" ".join(row)+'\n') for row in csv.reader(tot)]
#outputfile.close()
#tot.close()
