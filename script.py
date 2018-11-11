CLUSTER = False

if not CLUSTER:
    import findspark
    findspark.init()
from pyspark.sql import *

DATA_PATH = 'hdfs://datasets/amazon_multiling/tsv/' if CLUSTER else 'data/'

spark = SparkSession.builder.getOrCreate()

#data_electronics = spark.read.csv(DATA_PATH + 'sample_us.tsv')
data_electronics = spark.read.csv(DATA_PATH + 'amazon_reviews_multilingual_FR_v1_00.tsv')


print(data_electronics.take(3))

