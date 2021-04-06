#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  6 13:28:29 2021
@author: groth
"""

import pandas as pd
data = pd.read_json('yelp_academic_dataset_business.json', lines=True)
df = data.drop(['is_open', 'attributes', 'hours'], axis = 1).dropna(axis = 0)
df.reset_index(drop=True, inplace=True)
df.categories = df.categories.str.split(',')

flattened_col = pd.DataFrame(
[(index, value) for (index, values) in df.categories.iteritems() \
 for value in values], columns=['index', 'categories']).set_index('index')
flattened_col.categories = flattened_col.categories.str.strip()

indexes = flattened_col.index[flattened_col.categories.isin(['Restaurants'])]
restaurants = df[df.index.isin(indexes)]
restaurants.reset_index(drop=True, inplace=True)
restaurants = restaurants.drop(['categories'], axis = 1)

categories = pd.DataFrame(restaurants.business_id).join(flattened_col)
categories.reset_index(drop=True, inplace=True)

restaurants.to_json('yelp_business_restaurants.json')
categories.to_json('yelp_business_restaurants_categories.json')

review = pd.DataFrame()
chunks = pd.read_json('yelp_academic_dataset_review.json', lines=True, 
                      chunksize=50000)
chunk_list = []
for chunk in chunks:
    chunk_list.append(chunk)
    df = pd.concat(chunk_list)
review = review.append(df)

review = review.drop(['text'], axis = 1)
review_restaurants = review[review.business_id.isin(restaurants.business_id)]
review_restaurants.reset_index(drop=True, inplace=True)
review_restaurants.to_json('yelp_review_restaurants.json.gz', compression='gzip')

user = pd.DataFrame()
chunks = pd.read_json('yelp_academic_dataset_user.json', lines=True, 
                      chunksize=50000)
chunk_list = []
for chunk in chunks:
    chunk_list.append(chunk)
    df = pd.concat(chunk_list)
user = user.append(df)
user = user.drop(['elite', 'friends'], axis = 1)
user.to_json('yelp_user.json.gz', compression='gzip')




