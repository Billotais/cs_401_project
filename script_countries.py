import findspark
findspark.init()
from pyspark.sql import *
spark = SparkSession.builder.getOrCreate()

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import pyspark.sql.functions as f
from pyspark.sql.functions import to_date, dayofmonth, dayofweek, month, year, length, udf, greatest 

countries = ['US', 'UK', 'JP', 'FR', 'DE']

DATA_PATH = 'data/'
IMG_PATH = 'img/'


FILE_NAMES = ['amazon_reviews_multilingual_' + country + '_v1_00.tsv.gz' for country in countries]
datas = [spark.read.option("sep", "\t").option("header", "true").csv(DATA_PATH + FILE_NAME) for FILE_NAME in FILE_NAMES]


def generate_graphs(data_1, data_2, name_1, name_2):

    
generate_graphs(data_1, data_2, name_1, name_2)

data_pairs = zip(datas, datas)
name_pairs = zip(countries, countries)
print("Graphs generated for category %s" % name)