# Detecting Bias in Amazon reviews


## Abstract

In the past, when buying an item, one had to trust reviews in newspapers or from friends. In todays age, with online shopping, we have to tap into the minds of thousands of people who have purchased the product we are thinking about. With the help of Amazon reviews and their star-system, we can easily analyse how well to product likely is. But while a newspaper or professional reviewer is generally working hard for consistency and unbiasedness, these facts are not given for a general public reviewer writing a comment. With help of the Amazon dataset, we will try to find bias in the reviews, in order to possibly give an idea on whether or not, and if so how, to correct a bias. We will be especially interested in the influence of (some/all depending on the time) the following factors on the number of stars given:

- Time the review was written
- Type of product the review was written for
- Other informations about the product (Name, brand, also_bought, price, picture, ...)
- Content of review (length of text, number of 'helpful' votes, author, verified_purchase, ...)
- Eventually : influence of the language (if we can find some common products)


## Research questions

A list of research questions we would like to address during the project. 

- What factors influence the amount of stars given in an amazon review (other than product quality)?
- Is there a way to correct this bias? Should we correct this bias?
- Eventually : Should we trust the number of stars or the review itself to know how people really liked a product ? 

## Dataset

We will use the [Amazon Dataset](http://jmcauley.ucsd.edu/data/amazon/).

We will use NLP to extract interesting features from the product name and from the review text, and some image processing if we analyse the product picture

Since the dataset is very big, we will first concentrate on books, and possibly in a second stage extend our work to other categories if time permits. Since the dataset of books is still very large, we will use a subsample of the data for prototyping.


## A list of internal milestones up until project milestone 2

The main questions to answer in milestone 2:

-	How to best subsample the data (subsample books and take all reviews for 1 book or subsample reviews directly).
-	What features are at our disposal and how can we use them to answer our questions?
-	Is there a way to incorporate the metadata?
-	What features to extract by NLP from the review texts?

## Questions for TAs

The data on the cluster doesn't seem to correspond to what is shown on the dataset's webpage : Reviews and products seems to have already been merged on the cluster, which is not the case on the webpage. But some informations seem to be missing (image url, also_bought, brand, ...). Is this dataset (metadata) available on the cluster, or will we have to downlead it on our own computer ?
