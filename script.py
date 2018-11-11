from pyspark.sql import *
DATA_PATH = 'hdfs://datasets/amazon_multiling/tsv/'

spark = SparkSession.builder.getOrCreate()

data_electronics = spark.read.csv(DATA_PATH + 'sample_us.tsv')

print(data_electronics.take(3))

