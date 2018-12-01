import findspark
findspark.init()
from pyspark.sql import *
spark = SparkSession.builder.getOrCreate()

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import pyspark.sql.functions as f
from pyspark.sql.functions import to_date, dayofmonth, dayofweek, month, year, length, udf, greatest


# Files https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt'
categories = ['Wireless','Watches','Video_Games','Video_DVD','Video','Toys','Tools','Sports','Software','Shoes','Pet_Products','Personal_Care_Appliances','PC','Outdoors','Office_Products','Musical_Instruments','Music','Mobile_Electronics','Mobile_Apps','Major_Appliances','Luggage','Lawn_and_Garden','Kitchen','Jewelry','Home_Improvement','Home_Entertainment','Home','Health_Personal_Care','Grocery','Gift_Card','Furniture','Electronics','Digital_Video_Games','Digital_Video_Download','Digital_Software','Digital_Music_Purchase','Digital_Ebook_Purchase','Digital_Ebook_Purchase','Camera','Books','Books','Books','Beauty','Baby','Automotive','Apparel']
special_categories = [['Books_v1_00', 'Books_v1_01', 'Books_v1_02'], ['Digital_Ebook_Purchase_v1_00', 'Digital_Ebook_Purchase_v1_01']]
DATA_PATH = 'data/'
IMG_PATH = 'img/'



def generate_graphs(data, name):


    """Helpful votes vs total_votes"""

    # Creating the helpful ratio: helpful_votes / total_votes
    by_review = data.where('total_votes > 9').withColumn('helpful_ratio', data.helpful_votes / data.total_votes)
    count = by_review.count()

    # We will limit to at most ~100'000 data points in Pandas for pratical reasons
    if(count > 100_000):
        by_review = by_review.sample(False, 100_000 / count, 123456)
        
    # Data to panda and clean data
    by_review_pandas = by_review.toPandas()
    by_review_pandas = by_review_pandas[['helpful_ratio', 'star_rating']]
    by_review_pandas["star_rating"] = pd.to_numeric(by_review_pandas["star_rating"])

    # Group the reviews into 50 bins depending of their helpful_ratio
    by_review_pandas = by_review_pandas.groupby([pd.cut(by_review_pandas['helpful_ratio'], bins=50, include_lowest=True)]) \
                                    .agg({'star_rating' : ['mean', 'count', 'std'], 'helpful_ratio' : 'mean'})

    # Plot the results
    #fig, (ax1, ax2) = plt.subplots(1,2, figsize=(15,6))
        
    # First plot the average rating with its confidence interval
    plt.plot(by_review_pandas['helpful_ratio']['mean'], by_review_pandas['star_rating']['mean'])
    plt.fill_between(by_review_pandas['helpful_ratio']['mean'], by_review_pandas['star_rating']['mean'] - 1.96 * by_review_pandas['star_rating']['std'] / np.sqrt(by_review_pandas['star_rating']['count']), \
                                                             by_review_pandas['star_rating']['mean'] + 1.96 * by_review_pandas['star_rating']['std'] / np.sqrt(by_review_pandas['star_rating']['count']),alpha=0.5)
    plt.xlabel("Helpful ratio")
    plt.ylabel('Average Rating')
    plt.title('Average Rating vs Helpful ratio (with 95% CI) for category ' + name)
    plt.savefig(IMG_PATH + "helpful_vs_rating_" + name + ".png", bbox_inches='tight')  
    plt.clf()

    plt.plot(by_review_pandas['helpful_ratio']['mean'], by_review_pandas['star_rating']['count'])
    plt.xlabel("Helpful ratio")
    plt.ylabel('Number of reviews')
    plt.title('Number of reviews vs Helpful ratio for category ' + name)
    plt.savefig(IMG_PATH + "helpful_vs_number_" + name + ".png", bbox_inches='tight')      
    plt.clf()

    print("Helpfull votes done", end='\r')
    """Nr reviews pr product"""     

    # Group by product_id, get reviews count and average star_rating
    reviews_per_product = data.groupby('product_id').agg(f.avg('star_rating'), f.count('review_id'))
        
    # Rename columns
    reviews_per_product = reviews_per_product.withColumnRenamed('count(review_id)', 'n_reviews') \
                            .withColumnRenamed('avg(star_rating)', 'rating')
                
    reviews_per_product = reviews_per_product.where('n_reviews > 2')
    by_product = reviews_per_product.toPandas()

    # Group the data into logarithmic bins and get statistical information
    grouped_product_n_reviews = by_product.groupby([pd.cut(by_product['n_reviews'], bins=np.logspace(1, 3), include_lowest=True)]) \
                                      .agg({'rating' : ['mean', 'count', 'std'], 'n_reviews' : 'mean'})

    # Plot the results
    #fig, (ax1, ax2) = plt.subplots(1,2, figsize=(15,6))
        
    # First plot the average rating with its confidence interval
    plt.plot(grouped_product_n_reviews['n_reviews']['mean'], grouped_product_n_reviews['rating']['mean'])
    plt.fill_between(grouped_product_n_reviews['n_reviews']['mean'], grouped_product_n_reviews['rating']['mean'] - 1.96 * grouped_product_n_reviews['rating']['std'] / np.sqrt(grouped_product_n_reviews['rating']['count']), \
                                            grouped_product_n_reviews['rating']['mean'] + 1.96 * grouped_product_n_reviews['rating']['std'] / np.sqrt(grouped_product_n_reviews['rating']['count']), alpha=0.5)
    plt.xscale('log')
    plt.xlabel('Number of reviews per product')
    plt.ylabel('Average Rating')
    plt.title('Average Rating vs Number of reviews per product (with 95% CI) for category ' + name)
    plt.savefig(IMG_PATH + "rating_vs_reviews_" + name + ".png", bbox_inches='tight') 
    plt.clf()

    plt.plot(grouped_product_n_reviews['n_reviews']['mean'], grouped_product_n_reviews['rating']['count'])
    plt.xscale('log')
    plt.xlabel('Number of reviews per product')
    plt.ylabel('Number of products')
    plt.title('Number of products VS Number of reviews per product for category ' + name) 
    plt.savefig(IMG_PATH + "number_vs_reviews_" + name + ".png", bbox_inches='tight')  
    plt.clf()

    print("Number reviews done", end='\r')
    """Analysis of time based features"""

    def get_time_analysis(data, time_name):
        time_types = {'month' : month('date'), 'year' : year('date'), 'dayofweek' : dayofweek('date'), 'dayofmonth' : dayofmonth('date')}
        group_function = time_types[time_name]
        
        # Groupby the time type, aggregate usefull columns, and sort by time
        data_bytime = data.groupby(group_function).agg(f.avg('star_rating'), f.count('review_id'), f.stddev('star_rating')).withColumnRenamed('avg(star_rating)', 'rating').withColumnRenamed('stddev_samp(star_ratinng)', 'std_rating').withColumnRenamed(time_name + '(date)', time_name)
        data_bytime_sorted = data_bytime.sort(time_name)
        
        by_time_pd = data_bytime_sorted.toPandas()
        
        
        #fig, (ax1, ax2) = plt.subplots(1,2, figsize=(15,6))
        
        # First plot the average rating with it's condidence interval
        plt.plot(by_time_pd[time_name], by_time_pd['rating'])
        plt.fill_between(by_time_pd[time_name], by_time_pd['rating'] - 1.96 * by_time_pd['stddev_samp(star_rating)'] / np.sqrt(by_time_pd['count(review_id)']), \
                                             by_time_pd['rating'] + 1.96 * by_time_pd['stddev_samp(star_rating)'] / np.sqrt(by_time_pd['count(review_id)']), alpha=0.5)
        plt.xlabel(time_name)
        plt.ylabel('Average Rating')
        plt.title('Average Rating vs ' + time_name + ' for category ' + name)
        plt.savefig(IMG_PATH + "rating_by_" + time_name + "_evolution_"+ name + ".png", bbox_inches='tight')  
        plt.clf()

        plt.plot(by_time_pd[time_name], by_time_pd['count(review_id)'])
        plt.xlabel(time_name)
        plt.ylabel('Number of reviews')
        plt.title('Number of reviews vs ' + time_name + ' for category ' + name)
        plt.savefig(IMG_PATH + "number_by_" + time_name + "_evolution_"+ name + ".png", bbox_inches='tight')  
        plt.clf()

    by_time = data.select(data['star_rating'],to_date(data['review_date'], 'yyyy-MM-dd').alias('date'), data['review_id'])
    _ = by_time.persist()
    get_time_analysis(by_time, 'month')
    get_time_analysis(by_time, 'year')
    get_time_analysis(by_time, 'dayofweek')
    get_time_analysis(by_time, 'dayofmonth')
    print("Time features done", end='\r')
    """Text analysis"""

    group_by_3 = udf(lambda x : 0 if x is None else 3*int(x / 3))
    group_by_100 = udf(lambda x : 0 if x is None else 100*int(x / 100))

    # Titles grouped in bins of 3 characters, body grouped in bins of 100 characters
    by_review_length = data.select(data['star_rating'], \
                                group_by_3(length(data['review_headline'])).alias('title_length'), \
                                group_by_100(length(data['review_body'])).alias('body_length'), \
                                        data['review_id'])

    # We noticed that title_length is always between [0, 128] characters long
    by_title_length = by_review_length.where('title_length <= 128').groupby('title_length').agg(f.avg('star_rating'), f.count('review_id'), f.stddev('star_rating')) \
                        .withColumnRenamed('avg(star_rating)', 'rating') \
                        .withColumnRenamed('stddev_samp(star_rating)', 'std_rating') \
                        .withColumnRenamed('count(review_id)', 'count')
    by_title_length_pd = by_title_length.toPandas()
    by_title_length_pd["title_length"] = pd.to_numeric(by_title_length_pd["title_length"])
    by_title_length_pd = by_title_length_pd.sort_values(by='title_length')

    # Plot the results
    #fig, (ax1, ax2) = plt.subplots(1,2, figsize=(15,6))
        
    # First plot the average rating with its confidence interval
    plt.plot(by_title_length_pd['title_length'], by_title_length_pd['rating'])
    plt.fill_between(by_title_length_pd['title_length'], by_title_length_pd['rating'] - 1.96 * by_title_length_pd['std_rating'] / np.sqrt(by_title_length_pd['count']), \
                                            by_title_length_pd['rating'] + 1.96 * by_title_length_pd['std_rating'] / np.sqrt(by_title_length_pd['count']), alpha=0.5)
    plt.xlabel('Review title length')
    plt.ylabel('Average Rating')
    plt.title('Average Rating vs Review title length (with 95% CI) for category ' + name)
    plt.savefig(IMG_PATH + "rating_vs_title_length_" + name + ".png", bbox_inches='tight')
    plt.clf()

    plt.plot(by_title_length_pd['title_length'], by_title_length_pd['count'])
    plt.xlabel('Review title length')
    plt.ylabel('Number of reviews')
    plt.title('Number of reviews vs Review title length for category ' + name)
    plt.savefig(IMG_PATH + "number_vs_itle_length_" + name + ".png", bbox_inches='tight')  
    plt.clf()


    # We noticed that body_length is almost always between [0, 10000] characters long, after which there is almost no data which creates a huge amont of noise
    by_body_length = by_review_length.where('body_length <= 10000').groupby('body_length').agg(f.avg('star_rating'), f.count('review_id'), f.stddev('star_rating')) \
                        .withColumnRenamed('avg(star_rating)', 'rating') \
                        .withColumnRenamed('stddev_samp(star_rating)', 'std_rating') \
                        .withColumnRenamed('count(review_id)', 'count')
    by_body_length_pd = by_body_length.toPandas()

    by_body_length_pd["body_length"] = pd.to_numeric(by_body_length_pd["body_length"])
    by_body_length_pd = by_body_length_pd.sort_values(by='body_length')

    # Plot the results
    #fig, (ax1, ax2) = plt.subplots(1,2, figsize=(15,6))
        
    # First plot the average rating with its confidence interval
    plt.plot(by_body_length_pd['body_length'], by_body_length_pd['rating'])
    plt.fill_between(by_body_length_pd['body_length'], by_body_length_pd['rating'] - 1.96 * by_body_length_pd['std_rating'] / np.sqrt(by_body_length_pd['count']), \
                                           by_body_length_pd['rating'] + 1.96 * by_body_length_pd['std_rating'] / np.sqrt(by_body_length_pd['count']), alpha=0.5)
    plt.xlabel('Review body length')
    plt.ylabel('Average Rating')
    plt.title('Average Rating vs Review body length (with 95% CI) for category ' + name)
    plt.savefig(IMG_PATH + "rating_vs_review_length_" + name + ".png", bbox_inches='tight') 
    plt.clf()
     
    plt.plot(by_body_length_pd['body_length'], by_body_length_pd['count'])
    plt.xlabel('Review body length')
    plt.ylabel('Number of reviews')
    plt.title('Number of reviews vs Review body length for category ' + name)
    plt.savefig(IMG_PATH + "number_vs_review_length_" + name + ".png", bbox_inches='tight') 
    plt.clf()

    print("Text analysis done", end='\r')


for category in categories:
    FILE_NAME = 'amazon_reviews_us_' + category + '_v1_00.tsv.gz'
    data = spark.read.option("sep", "\t").option("header", "true").csv(DATA_PATH + FILE_NAME)
    name = ' '.join(category.split('_'))
    generate_graphs(data, name)
    print("Graphs generated for category %s" % name)

for category in special_categories:
    FILE_NAMES = ['amazon_reviews_us_' + x + '.tsv.gz' for x in category]
    print(FILE_NAMES)
    datas = [spark.read.option("sep", "\t").option("header", "true").csv(DATA_PATH + FILE_NAME) for FILE_NAME in FILE_NAMES]
    data = datas[0]
    for i in datas[1:]:
        data = data.union(i)
    name = ' '.join(category[0][:-6].split('_'))
    generate_graphs(data, name)
    print("Graphs generated for category %s" % name)