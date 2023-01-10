import pandas as pd

df = pd.read_csv('/Users/pangli/Desktop/yelp_training_set_review.csv')

df.drop(index=[14021])

df.to_csv('data/yelp_training_set_review.csv')