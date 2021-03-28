#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from mysql.connector import connect, Error
from getpass import getpass

b = pd.read_json('yelp_business_restaurants.json')
b = b.drop('categories', axis = 1)

b_denorm = pd.read_json('yelp_business_denormalized.json.gz', 
                        compression = 'gzip')

def width(df, attribute):
    return max([len(x) for x in df[attribute]])

widths = [width(b, 'business_id'),
          width(b, 'name'),
          width(b, 'address'),
          width(b, 'city'),
          width(b, 'state'),
          width(b, 'postal_code')]

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

# check that the schema is correct
# try:
#     with connection.cursor() as cursor:
#         cursor.execute('describe business')
#         result = cursor.fetchall()
#         for r in result:
#             print(r)
# except Error as e:
#     print(e)

create_denormalized_table = '''
create table business_denormalized like business
'''

add_categories_column = '''
alter table business_denormalized 
add column category varchar({})
'''.format(max([len(x) for x in b_denorm.categories]))

try:
    with connection.cursor() as cursor:
        cursor.execute(create_denormalized_table)
        cursor.execute(add_categories_column)
        connection.commit()
except Error as e:
    print(e)
    

# show tables
try:
    with connection.cursor() as cursor:
        cursor.execute('show tables')
        result = cursor.fetchall()
        for r in result:
            print(r)
except Error as e:
    print(e)


business_values = [row for row in b.itertuples(index=False, name=None)]
denormalized_values = [row for row in b_denorm.itertuples(index=False, name=None)]

insert_into_business = '''
insert into business
(business_id, name, address, city, state, postal_code,
 latitude, longitude, stars, review_count)
values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

insert_into_denormalized = '''
insert into business_denormalized
(business_id, name, address, city, state, postal_code,
 latitude, longitude, stars, review_count, category)
values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

with connection.cursor() as cursor:
    cursor.executemany(insert_into_business, business_values)
    cursor.executemany(insert_into_denormalized, denormalized_values)
    connection.commit()

# check that all rows were entered: 63944 and 274306
with connection.cursor() as cursor:
    cursor.execute('select count(*) from business')
    result = cursor.fetchall()
    for r in result:
        print(r)

with connection.cursor() as cursor:
    cursor.execute('select count(*) from business_denormalized')
    result = cursor.fetchall()
    for r in result:
        print(r)

connection.close()



