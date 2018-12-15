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




def generate_graphs(data_1, data_2, name_1, name_2):
    def group_by_product(data):
        avg_stars = data.groupby('product_id', 'product_title').agg(f.avg('star_rating'), 
                                                                    f.count('review_id'), 
                                                                    f.min(to_date(data['review_date'], 'yyyy-MM-dd')),
                                                                    f.stddev('star_rating'))
        avg_stars = avg_stars.withColumnRenamed('count(review_id)', 'n_reviews') \
                            .withColumnRenamed('avg(star_rating)', 'rating') \
                            .withColumnRenamed("min(to_date(`review_date`, 'yyyy-MM-dd'))", 'first_date') \
                            .withColumnRenamed('stddev_samp(star_rating)', 'std_rating')
        return avg_stars
        
    avg_1 = group_by_product(data_1)
    avg_2 = group_by_product(data_2)


    # To be able to differenciate columns after a later join
    c1 = avg_1.alias("c1")
    c2 = avg_2.alias("c2")

    c1_c2 = c1.join(c2, f.col('c1.product_id') == f.col('c2.product_id'))
    latest_date = c1_c2.select(f.col('c1.product_id'),greatest(f.col('c1.first_date'), f.col('c2.first_date'))) \
                        .withColumnRenamed("product_id", "id").withColumnRenamed("greatest(c1.first_date, c2.first_date)", 'latest_date')

    c1_common_with_date = data_1.join(latest_date, data_1['product_id'] == latest_date['id'])
    c1_common_reviews = c1_common_with_date.where('review_date >= latest_date')

    c2_common_with_date = data_2.join(latest_date, data_2['product_id'] == latest_date['id'])
    c2_common_reviews = c2_common_with_date.where('review_date >= latest_date')

    common_c1_avg = group_by_product(c1_common_reviews)
    common_c2_avg = group_by_product(c2_common_reviews)

    c1_pd = common_c1_avg.toPandas()
    c2_pd = common_c2_avg.toPandas()

    plt.figure(figsize=(10,6))
    plt.boxplot([c1_pd['rating'], c2_pd['rating']], 0, sym='',autorange=True, labels=[name_1, name_2])
    plt.title('Distribution of the average ratings / product - '+ name_1 + " vs " + name_2)
    plt.ylabel('Average rating')
    plt.ylim(2.4, 5.1)
    plt.savefig(IMG_PATH + "countries/average_rating_" + name_1 + "_" + name_2 + ".png", bbox_inches='tight')
    plt.clf()
    

    

datas = {country : spark.read.option("sep", "\t").option("header", "true").csv(DATA_PATH + 'amazon_reviews_multilingual_' + country + '_v1_00.tsv.gz') for country in countries}


name_pairs = [(a, b) for a in countries for b in countries if countries.index(a) < countries.index(b)]


for (a, b) in name_pairs:
    generate_graphs(datas[a], datas[b], a, b)

