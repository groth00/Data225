#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 19:25:57 2021

@author: groth
"""

import pandas as pd
from mysql.connector import connect, Error
from getpass import getpass

b = pd.read_json('yelp_business_restaurants.json')
flattened_col = pd.DataFrame(
[(index, value) for (index, values) in b.categories.iteritems() \
 for value in values], columns=['index', 'categories']).set_index('index')
temp = b.drop(['name', 'address', 'city', 'state', 'postal_code',
               'latitude', 'longitude', 'stars', 'review_count',
               'categories'], axis = 1).join(flattened_col)
b = b.drop('categories', axis = 1)

def width(df, attribute):
    return max([len(x) for x in df[attribute]])

widths = [width(b, 'business_id'),
          width(b, 'name'),
          width(b, 'address'),
          width(b, 'city'),
          width(b, 'state'),
          width(b, 'postal_code'),]

try:
    connection = connect(host="localhost", 
                         user=input("Enter username: "),
                         password=getpass("Enter password: "),)
except Error as e:
    print(e)

try:
    with connection.cursor() as cursor:
        cursor.execute('drop database if exists yelp')
        cursor.execute('create database if not exists yelp')
except Error as e:
    print(e)

try:
    with connection.cursor() as cursor:
        cursor.execute('use yelp')
except Error as e:
    print(e)
    
create_business_table = '''
create table if not exists business(
business_id char({}),
name varchar({}),
address varchar({}),
city varchar({}),
state varchar({}),
postal_code varchar({}),
latitude float(10,7),
longitude float(10,7),
stars float(2,1),
review_count int,
primary key(business_id, name)
)
'''.format(*widths)

try:
    with connection.cursor() as cursor:
        cursor.execute(create_business_table)
        connection.commit()
except Error as e:
    print('error creating business table: ', e)

create_categories_table = '''
create table categories(
id integer auto_increment primary key,
business_id char({}),
category varchar({}),
foreign key(business_id) references business(business_id)
)
'''.format(widths[0], max([len(x) for x in flattened_col.categories]))

try:
    with connection.cursor() as cursor:
        cursor.execute(create_categories_table)
        connection.commit()
except Error as e:
    print('error creating categories table: ', e)
    
# show tables
try:
    with connection.cursor() as cursor:
        cursor.execute('show tables')
        result = cursor.fetchall()
        print('tables in yelp database include: ')
        for r in result:
            print(r)
except Error as e:
    print(e)

################################################
########### Load data into tables ##############
################################################

business_values = [row for row in b.itertuples(index=False, name=None)]
category_values = [row for row in temp.itertuples(index=False, name=None)]

insert_into_business = '''
insert into business
(business_id, name, address, city, state, postal_code,
 latitude, longitude, stars, review_count)
values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

insert_into_categories = '''
insert into categories
(business_id, category)
values(%s, %s)
'''
try:
    with connection.cursor() as cursor:
        cursor.executemany(insert_into_business, business_values)
        connection.commit()
except Error as e:
    print('error inserting values into business table: ', e)
    
try:
    with connection.cursor() as cursor:
        cursor.executemany(insert_into_categories, category_values)
        connection.commit()
except Error as e:
    print('error inserting values into categories table: ', e)
    

# check that all rows were entered: 63944 and 274306
with connection.cursor() as cursor:
    cursor.execute('select count(*) from business')
    result = cursor.fetchall()
    for r in result:
        print(f'There are {r} rows in table: business')
        
with connection.cursor() as cursor:
    cursor.execute('select count(*) from categories')
    result = cursor.fetchall()
    for r in result:
        print(f'There are {r} rows in table: categories')

################################################
################### Analysis ###################
################################################

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
order by num_restaurants desc;
'''
fetchQuery(businesses_per_state, "businesses_per_state")

avg_stars_query = '''
select category, avg(stars)
from business join categories using(business_id)
group by category
having count(*) > 50
'''
fetchQuery(avg_stars_query, "groups businesses by category and returns avg star rating for group size > 50")

top_20 = '''
select * from business order by stars DESC limit 20;
'''
fetchQuery(top_20, "top 20 businesses ranked by stars")

bottom_20 = '''
select * from business order by stars limit 20;
'''
fetchQuery(bottom_20, "bottom 20 businesses ranked by stars")

fetchQuery("select name, address,count(*) as cnt from business group by name, address having cnt > 2 order by cnt DESC", \
            "businesses that have more than one row for the same name and address")

fetchQuery("select * from business where name='Thai Thai Restaurant' and address='10890 S Eastern Ave, Ste 109';", \
            "entries for the same Thai Thai Restaurant")

fetchQuery("select * from business where name='Papa John\\'s Pizza' and address='5570 Camino Al Norte, Ste D2';", \
            "entries for the same Papa John's")

connection.close()