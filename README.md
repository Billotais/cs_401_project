# Detecting Bias in Amazon reviews


## Abstract

In the past, when buying an item, one had to trust reviews in newspapers or from friends. In todays age, with online shopping, we have to tap into the minds of thousands of people who have purchased the product we are thinking about. With the help of Amazon reviews and their star-system, we can easily analyse how well to product likely is. But while a newspaper or professional reviewer is generally working hard for consistency and unbiasedness, these facts are not given for a general public reviewer writing a comment. With help of the Amazon dataset, we will try to find bias in the reviews, in order to possibly give an idea on whether or not, and if so how, to correct a bias. We will be especially interested in the influence of the following factors on the number of stars given:

- Time the review was written
- Type of product the review was written for
- Style, content and length of the product name and description
- Product Price
- Potentially product picture
- Potentially shoppers activity


## Research questions

A list of research questions we would like to address during the project. 

- What factors influence the amount of stars given in an amazon review (other than product quality)?
- Is there a way to correct this bias? Should we correct this bias?
- Eventually : Should we trust the number of stars or the review content more to know how people liked a product ? 

## Dataset

We will use the [Amazon Dataset](http://jmcauley.ucsd.edu/data/amazon/).

We will use NLP to extract interesting features from the description text, and from the product name. We might also use some features directly from the review text.

Since the dataset is very big, we will first concentrate on books, and possibly in a second stage extend our work to other categories if time permits. Since the dataset of books is still very large, we will use a subsample of the data for prototyping.


## A list of internal milestones up until project milestone 2

The main questions to answer in milestone 2:

-	How to best subsample the data (subsample books and take all reviews for 1 book or subsample reviews directly).
-	What features are at our disposal and how can we use them to answer our questions?
-	Is there a way to incorporate the metadata?
-	What features to extract by NLP from the description and review texts?

## Questions for TAs

Add here some questions you have for us, in general or project-specific.
