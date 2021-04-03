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
        cursor.execute('create database if not exists yelp')
except Error as e:
    print(e)

try:
    with connection.cursor() as cursor:
        cursor.execute('use yelp')
except Error as e:
    print(e)
    
try:
    with connection.cursor() as cursor:
        cursor.execute('drop table if exists business')
        cursor.execute('drop table if exists business_denormalized')
        cursor.execute('drop table if exists categories')
except Error as e:
    print(e)

create_business_table = '''
create table if not exists business(
id int auto_increment primary key,
business_id char({}),
name varchar({}),
address varchar({}),
city varchar({}),
state varchar({}),
postal_code varchar({}),
latitude float(10,7),
longitude float(10,7),
stars float(2,1),
review_count int)
'''.format(*widths)

try:
    with connection.cursor() as cursor:
        cursor.execute(create_business_table)
        connection.commit()
except Error as e:
    print(e)

create_categories_table = '''
create table categories(
business_id char({}),
category varchar({})
)
'''.format(widths[0], max([len(x) for x in flattened_col.categories]))

try:
    with connection.cursor() as cursor:
        cursor.execute(create_categories_table)
        connection.commit()
except Error as e:
    print(e)
    
# show tables
try:
    with connection.cursor() as cursor:
        cursor.execute('show tables')
        result = cursor.fetchall()
        print(f'tables in yelp database include: ')
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

with connection.cursor() as cursor:
    cursor.executemany(insert_into_business, business_values)
    cursor.executemany(insert_into_categories, category_values)
    connection.commit()

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

# some queries
businesses_per_state = '''
select state, count(*) as num_restaurants
from business
group by state
order by num_restaurants desc;
'''
with connection.cursor() as cursor:
    cursor.execute(businesses_per_state)
    result = cursor.fetchall()
    for r in result:
        print(r)

''' groups businesses by category and returns avg star rating
for group size > 50
'''

avg_stars_query = '''
select category, avg(stars)
from business join categories using(business_id)
group by category
having count(*) > 50
'''
with connection.cursor() as cursor:
    cursor.execute(avg_stars_query)
    result = cursor.fetchall()
    for r in result:
        print(r)

connection.close()