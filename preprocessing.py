#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 13 15:44:14 2021
@author: groth
"""

import pandas as pd
from collections import Counter

data = pd.read_json('yelp_academic_dataset_business.json', lines=True)

# drop the columns is_open, attributes, hours
to_drop = [data.columns.get_loc(x) for x in ('is_open', 'attributes', 'hours')]
df = data.drop(data.columns[to_drop], axis = 1).dropna(axis = 0)
df.reset_index(drop=True, inplace=True)
df.categories = df.categories.str.split(',')

# remove whitespace from each category
for i, list_of_categories in enumerate(df.categories):
    for j, category in enumerate(list_of_categories):
        df.categories[i][j] = category.strip()

# find the most populated business categories
category_list = []
for list_of_categories in df.categories:
    for category in list_of_categories:
        category_list.append(category)

freq = Counter(category_list)
print(freq.most_common(20))

def get_idx_for_business_category(df, category=None):
    '''
    input: df: a dataframe
    category: a string
    
    get row indexes for businesses pertaining to ONE category
    if the category does not exist, this returns an empty list
    '''
    if not category: return None
    indexes = []
    for i in range(len(df)):
        try:
            if category.capitalize() in df.categories[i]:
                indexes.append(i)
        except TypeError: #ignore bad values such as nan
            continue
        except KeyError:
            continue
    return indexes
idx = get_idx_for_business_category(df, 'restaurants')

# get the subset of rows for restaurant businesses
restaurants = df[df.index.isin(idx)]
restaurants.reset_index(drop=True, inplace=True)

# restaurants.to_json('yelp_business_restaurants.json')

# filter by review count
restaurants_50 = restaurants[restaurants.review_count > 50]

# get the business IDs of restaurants
r_id = restaurants.business_id

# reviews = pd.read_json('yelp_academic_dataset_review.json', lines=True)
reviews = pd.read_json('yelp_reviews_restaurants_with_name.json')

# get reviews for restaurants only
r_reviews = reviews[reviews.business_id.isin(r_id)]
r_reviews.reset_index(drop=True, inplace=True)

# join reviews, restaurants to add name of restaurant to each review
rest_0 = restaurants[['business_id', 'name']]
temp = r_reviews.join(rest_0.set_index('business_id'), on = 'business_id')


# save the updated reviews as a gzip compressed json file
# pandas to_string() truncuates long strings, so the above method is better

# temp.to_json('yelp_reviews_50.json.gz, compression='gzip')

        

    
        


