#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 19:25:57 2021
Most recent edit on April 6 15:38 2021
@author: groth
"""

import pandas as pd
from mysql.connector import connect, Error
from getpass import getpass

business_metadata = pd.read_json('yelp_business_restaurants.json')
business_categories = pd.read_json('yelp_business_restaurants_categories.json')

def width(df, attributes):
    return[max([len(x) for x in business_metadata[attr]]) for attr in attributes]

widths = width(business_metadata, ['business_id', 'name', 'address',
                                   'city', 'state', 'postal_code'])
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
        cursor.execute('use yelp')
except Error as e:
    print(e)
    
create_business_table = '''
create table if not exists business(
business_id char({}) primary key,
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

create_categories_table = '''
create table categories(
id integer auto_increment primary key,
business_id char({}),
category varchar({}),
foreign key(business_id) references business(business_id)
)
'''.format(widths[0], max([len(x) for x in business_categories.categories]))

create_user_table = '''
create table user(
user_id varchar(22) primary key,
name varchar(30),
review_count integer,
yelping_since datetime,
useful integer,
funny integer,
cool integer,
fans integer,
average_stars float(3, 2),
compliment_hot integer,
compliment_more integer,
compliment_profile integer,
compliment_cute integer,
compliment_list integer,
compliment_note integer,
compliment_plain integer,
compliment_cool integer,
compliment_funny integer,
compliment_writer integer,
compliment_photos integer)
'''

create_review_table = '''
create table review(
review_id varchar(22) primary key,
user_id varchar(22),
business_id varchar(22),
stars integer,
date_of datetime,
useful int,
funny int,
cool int,
foreign key(user_id) references user(user_id),
foreign key(business_id) references business(business_id))
'''

try:
    with connection.cursor() as cursor:
        cursor.execute(create_business_table)
        cursor.execute(create_categories_table)
        cursor.execute(create_user_table)
        cursor.execute(create_review_table)
        connection.commit()
except Error as e:
    print('error creating a table: ', e)

    
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

business_values = list(business_metadata.itertuples(index=False, name=None))
category_values = list(business_categories.itertuples(index=False, name=None))

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
select_statements = '''
select count(*) from business;
select count(*) from categories;
'''
with connection.cursor() as cursor:
    for cur in cursor.execute(select_statements, multi=True):
        if cur.with_rows:
            print(cur.fetchall())

connection.close()