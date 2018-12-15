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


# Milestones

## Milestone 1

### A list of internal milestones up until project milestone 2

The main questions to answer in milestone 2:

-	How to best subsample the data (subsample books and take all reviews for 1 book or subsample reviews directly).
-	What features are at our disposal and how can we use them to answer our questions?
-	Is there a way to incorporate the metadata?
-	What features to extract by NLP from the review texts?

## Questions for TAs

The data on the cluster doesn't seem to correspond to what is shown on the dataset's webpage : Reviews and products seems to have already been merged on the cluster, which is not the case on the webpage. But some informations seem to be missing (image url, also_bought, brand, ...). Is this dataset (metadata) available on the cluster, or will we have to downloaded it on our own computer ?

## Milestone 2

### Some ideas about the correction of potential bias

Define avg = Median of all the users' average rating (only taking account users with a minimal amount of reviews to get more significant users rating habits)

Now, for a given review:

- Let x = the average rating for the user that wrote the review
- Let *alpha*, a coefficient from 0 to 1, depending on the number of votes and the ratio of helpful votes s.t.
    - The higher the helpful ratio is, the lower the coefficient will be
    - If the number of votes is very small (~1-3?), the impact of the helpful ratio will be lessened
    - We might have other parameters influencing *alpha*, in which case we will update this formula accordingly 
	- A possible formula for *alpha* is *alpha=[helpful ratio] * (1-exp(-lambda * [n_reviews]))* where *lambda* is a tuning parameter.
    - The tuning parameter *lambda* might also depend on other variables, such at the time of the review, or a potential herding behavior
- We can now get the corrected rating as *corrected_rating = actual_rating + alpha * (avg - x)*.


We can explain what this does like this: when we see a review, we will look at the rating habits of the user. If the user is used to giving bad ratings compared to the amazon average, he will be considered as biased, and his rating will be revised upwards. However, if his review received a lot of helpful votes, it probably means that other customers agreed with him, and in that case we will not change his rating as much (hence *alpha* will be closer to 0). The idea is similar for reviewers that mostly put positive reviews, in which case it will be revised downwards.

Overall we do not really know how it will affect the total average rating of amazon, as we are taking the *x* as the median (and not the average), it may not affect symmetrically the users above/under the *x* value. Also we only take the users with a minimal amount of reviews into account, but they may act differently than the other users (for instance give more extreme values...). The alpha value may also skew the results as users will possibly set more positive reviews as helpful than negative ones.

Note that the corrected rating might be greater than 5. We will temporarily keep that value, and then compute the average rating for an article, such that ratings that were greatly pushed upwards (e.g. to a 5.6) will have a bigger positive effect than the others "best ratings" of 5 stars. For the final rating of the product, we could either put all the ratings higher than 5 at 5, or do some scaling over all the reviews to but most of them in the range from 1 to 5 (e.g. if 95% of the corrected product ratings are between 1 and 6, we can either cut all the ratings from 5 to 6 and put them at 5, or shift/scale all the ratings so that a 6 now corresponds to a 5, and a 5 now corresponds to a ~4.17)   


### Other analysis

We will also perform analysis of the average ratings across time, countries and product types. We will use the average rating as a proxy for raters happyness and how severe they rate.
We argue that this analysis can be done because there is no reason for the same product to be rated differently accross different countries.
In milestone 2 we show how we will conduct our analysis and test it for US-books. In the next milestone, we will extend it to different countries and product types.
Herding behaviour is another effect we will consider in the next milestone.

## Milestone 3

In milestone 3, we performed a more detailed by-country analysis. In particular we looked at the herding effect. We also implemented our bias correction formula.

The new results were added to the notebook, and a datastory is available here : https://ada-lyn.github.io/
