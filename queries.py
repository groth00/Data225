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
group by category
having count(*) > 50
order by avg_stars desc
limit 20
'''
fetchQuery(avg_stars_query, '''
average star rating for each category of business having size > 50''')

top_20 ='''select name, address, city, state, stars, review_count 
from business 
order by stars desc
limit 20'''
fetchQuery(top_20, "top 20 businesses ranked by stars")

bottom_20 = '''select name, address, city, state, stars, review_count 
from business 
order by stars 
limit 20'''
fetchQuery(bottom_20, "bottom 20 businesses ranked by stars")

connection.close()
