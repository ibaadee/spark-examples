import sys
import csv
from pyspark import SparkContext
from pyspark.ml.clustering import KMeans

# if __name__ == "__main__":
	# master = "local"
	# if len(sys.argv) == 2:
	# 	master = sys.argv[1]
	
	# sc = SparkContext(master, "SimpleKMeans")	
	
	# Loads data.
	# dataset = spark.read.format("libsvm").load("FileStore/tables/season_output_2006.csv")
dataset = sc.textFile("FileStore/tables/season_output_2006.csv")
parsedData = dataset.map(lambda line: array([float(x) for x in line.split(',')]))


spark_rdd = df.rdd.map(lambda row: (row["ID"], Vectors.dense(row["Latitude"],row["Longitude"])))
feature_df = spark_rdd.toDF(["ID", "features"])
    # feature_df = spark_rdd.toDF(["ID", "features"])

	# rows = csv.map(line.split(",").map(_.trim))
	# header = rows.first
	# data = rows.filter(_(0) != header(0))
	# rdd = data.map(Row(row(0),row(1).toInt))
	
	# schema = new StructType()
    # .add(StructField("itemtype", IntegerType, true))
    # .add(StructField("collection", IntegerType, true))
	# .add(StructField("checkoutdatetime", IntegerType, true))
	# .add(StructField("season", IntegerType, true))
	
	# df = sqlContext.createDataFrame(spark_rdd, schema)
	
	# dataset = sc.textFile("FileStore/tables/season_output_2006.csv").map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[1])).collect()
	
	# df = sqlContext.createDataFrame(rdd, dataset)
	
	# Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(parsedData)

	# Evaluate clustering by computing Within Set Sum of Squared Errors.
wssse = model.computeCost(parsedData)
print("Within Set Sum of Squared Errors = " + str(wssse))

	# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
	print(center)
