CLUSTER = True

if not CLUSTER:
    import findspark
    findspark.init()
from pyspark.sql import *

DATA_PATH = 'hdfs:///datasets/amazon_multiling/tsv/' if CLUSTER else 'data/'

spark = SparkSession.builder.getOrCreate()

#data_electronics = spark.read.csv(DATA_PATH + 'sample_us.tsv')
data_reviews = spark.read.csv(DATA_PATH + 'amazon_reviews_multilingual_FR_v1_00.tsv.gz')

first_10 = data_reviews.take(10)
for row in first_10:
	print(row)

