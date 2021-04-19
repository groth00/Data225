create table business(
business_id char(22) primary key,
name varchar(54),
address varchar(80),
city varchar(24),
state varchar(2),
postal_code varchar(5),
latitude decimal(10,7),
longitude decimal(10,7),
stars decimal(2,1),
review_count int);

create table categories(
id int identity(1, 1),
business_id varchar(22),
category varchar(32),
foreign key(business_id) references business(business_id));

create table user_info(
id integer,
user_id varchar(22) primary key,
name varchar(60),
review_count integer,
yelping_since datetime,
useful integer,
funny integer,
cool integer,
fans integer,
average_stars decimal(3, 2),
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
compliment_photos integer);
                                             
create table review(
id integer,
review_id varchar(22) primary key,
user_id varchar(22),
business_id varchar(22),
stars int,
useful int,
funny int,
cool int,
date_of datetime,
foreign key(business_id) references business(business_id),
foreign key(user_id) references user_info(user_id));

copy business
from 's3://BUCKET/FILENAME.csv'
credentials 'aws_access_key_id=;aws_secret_access_key='
ignoreheader 1
csv;

copy categories
from 's3://BUCKET/FILENAME.csv'
credentials 'aws_access_key_id=;aws_secret_access_key='
ignoreheader 1
csv;

copy user_info
from 's3://BUCKET/FILENAME.csv'
credentials 'aws_access_key_id=;aws_secret_access_key='
ignoreheader 1
csv;

copy review
from 's3://BUCKET/FILENAME.csv'
credentials 'aws_access_key_id=;aws_secret_access_key='
ignoreheader 1
csv;
                 