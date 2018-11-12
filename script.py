CLUSTER = False

if not CLUSTER:
    import findspark
    findspark.init()
from pyspark.sql import *

FILE_NAME = 'amazon_reviews_multilingual_FR_v1_00.tsv.gz'
DATA_PATH = 'hdfs://datasets/amazon_multiling/tsv/' if CLUSTER else 'data/'

spark = SparkSession.builder.getOrCreate()


data = spark.read.option("sep", "\t").option("header", "true").csv(DATA_PATH + FILE_NAME)


print(data.take(2))
print(data.count())
