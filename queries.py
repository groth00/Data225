#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 6 15:31:14 2021
@author: groth
"""

from mysql.connector import connect, Error
from getpass import getpass

try:
    connection = connect(host="localhost", 
                         user=input("Enter username: "),
                         password=getpass("Enter password: "),
                         database='yelp')
except Error as e:
    print(e)

def fetchQuery(query: str, description: str = "no description") -> None:
    print(description)
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        for r in result:
            print(r)
    print()

# some queries
businesses_per_state = '''
select state, count(*) as num_restaurants
from business
group by state
order by num_restaurants desc
'''
fetchQuery(businesses_per_state, "businesses_per_state")

avg_stars_query = '''
select category, avg(stars) as avg_stars
from business join categories using(business_id)
where state = 'MA'
group by category
having count(*) > 50
order by avg_stars desc
limit 20
'''
fetchQuery(avg_stars_query, '''
average star rating for each category of business having size > 50''')

top_20 = '''select name, address, city, state, stars, review_count 
from business
where state = 'MA'
order by stars desc
limit 20'''
fetchQuery(top_20, "top 20 businesses ranked by stars")

bottom_20 = '''select name, address, city, state, stars, review_count 
from business
where state = 'MA' 
order by stars 
limit 20'''
fetchQuery(bottom_20, "bottom 20 businesses ranked by stars")

star_rating = '''select stars AS Star_rating, count(stars) AS Count from business
where state = 'MA'
group by Star_rating
order by Star_rating desc
limit 10'''
fetchQuery(star_rating, "Summary of star_rating in MA")

categories_restaurants = '''with categories_full AS (select name, category, address, city, state, stars, review_count 
from business B, categories C
where B.business_id = C.business_id AND state = 'MA'
)
select count(1) as cnt, name, address, city, state
from categories_full
group by name, address, city, state
order by cnt DESC'''
fetchQuery(categories_restaurants, "number of categories each business belong to")

categories_MA = '''with categories_full AS (select name, category, address, city, state, stars, review_count 
from business B, categories C
where B.business_id = C.business_id AND state = 'MA')
select *
from categories_full
where stars=5.0
order by review_count DESC'''
fetchQuery(categories_MA, "Categories of the most reviewed 5-star restaurants in MA")

top_cities = '''select count(1) cnt, city 
from business
where state = 'MA'
group by city
order by cnt desc'''
fetchQuery(top_cities, "Top cities with the most restaurants in MA")

connection.close()
