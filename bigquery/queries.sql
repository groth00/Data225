-- 2019 vs 2020 Yelp Registration Number & Review Number
SELECT month,T1.year as year2019,T1.registerNum as RegisterNum2019, T11.reviewNum as ReviewNum2019,
    T2.year as year2020, T2.registerNum as RegisterNum2020, T22.reviewNum as ReviewNum2020,
FROM    (SELECT   
    EXTRACT(MONTH from yelping_since ) month,
    EXTRACT(YEAR from yelping_since ) year,
    count(user_id) as registerNum,
    FROM  `united-helix-310720.yelp.users`
    Where EXTRACT(YEAR from yelping_since ) = 2019
    GROUP BY month, year) T1
JOIN (SELECT 
    EXTRACT(MONTH from date ) month,
    EXTRACT(YEAR from date ) year,
    count(review_id) as reviewNum    
    FROM  `united-helix-310720.yelp.reviews`
    Where EXTRACT(YEAR from date ) = 2019
    GROUP BY month, year) T11
    USING(month)    
JOIN ( SELECT EXTRACT(MONTH from yelping_since ) month,
    EXTRACT(YEAR from yelping_since ) year,
    count(user_id) as registerNum    
    FROM  `united-helix-310720.yelp.users`
    Where EXTRACT(YEAR from yelping_since ) = 2020
    GROUP BY month, year) T2
    USING(month)    
JOIN (SELECT 
    EXTRACT(MONTH from date ) month,
    EXTRACT(YEAR from date ) year,
    count(review_id) as reviewNum    
    FROM  `united-helix-310720.yelp.reviews`
    Where EXTRACT(YEAR from date ) = 2020
    GROUP BY month, year) T22
    USING(month)
GROUP BY month,T1.year, T1.registerNum,T2.year, T2.registerNum , T11.reviewNum, T22.reviewNum  
ORDER BY month;

-- total number of reviews submitted by Yelp users (writers) in each hour.
SELECT T.hour,TotalNum,UsefulNum,FunnyNum,CoolNum
FROM
    (select EXTRACT(HOUR from date) as hour,
    count( user_id ) TotalNum 
    from `united-helix-310720.yelp.reviews` 
    group by hour) T    
 join (
      select EXTRACT(HOUR from date ) hour,
      sum( useful ) UsefulNum 
      FROM  `united-helix-310720.yelp.reviews` 
      group by hour) 
      using (hour)
 join (
      select EXTRACT(HOUR from date ) hour,
      sum( funny ) FunnyNum
      FROM  `united-helix-310720.yelp.reviews` 
      group by hour) 
      using (hour)      
 join (
      select EXTRACT(HOUR from date ) hour,
       sum( cool ) CoolNum
      FROM  `united-helix-310720.yelp.reviews` 
      group by hour) 
      using (hour)
 group by T.hour,TotalNum,UsefulNum,FunnyNum,CoolNum
 order by hour;   

-- sentiment percentages
select B.name, A.string_field_2 as sentiment, 
count(A.string_field_2) as sentiment_count
from `united-helix-310720.yelp.yelp-restaurants` B,`united-helix-310720.yelp.annotation` A
where B.business_id = A.string_field_0
group by B.name, A.string_field_2;

-- sentiment of "Atmosphere"
select B.name, A.string_field_2 as sentiment, 
count(A.string_field_2) as sentiment_count
from `united-helix-310720.yelp.yelp-restaurants` B,`united-helix-310720.yelp.annotation` A
where B.business_id = A.string_field_0 and A.string_field_1 = 'atmosphere'
group by B.name, A.string_field_2;

-- sentiment towards "Services"
select B.name, A.string_field_2 as sentiment, 
count(A.string_field_2) as sentiment_count
from `united-helix-310720.yelp.yelp-restaurants` B,`united-helix-310720.yelp.annotation` A
where B.business_id = A.string_field_0 and A.string_field_1 = 'service'
group by B.name, A.string_field_2;

-- review counts grouped by month per year
select extract(year from R.date) year, extract(month from R.date) month, count(*) review_count
from `united-helix-310720.yelp.reviews` R
where extract(year from R.date) between 2006 and 2020 
group by year, month;

-- usefulness of users (user scores)
select U.name, extract(year from U.yelping_since) year_joined, 
U.useful, U.review_count, U.useful/U.review_count as score
from `united-helix-310720.yelp.users` U
where U.review_count > 10
order by useful desc, score desc;

-- cities with most businesses
select business_cnt, city from (
select 
  count(distinct rest.business_id) business_cnt,
  rest.city
 from (
SELECT
  user_id
FROM
     `united-helix-310720.yelp.users` user
  where FLOOR(average_stars) = 3
) t1 
join `united-helix-310720.yelp.reviews` review 
on t1.user_id = review.user_id
join   `united-helix-310720.yelp.yelp-restaurants` rest
on review.business_id = rest.business_id
group by rest.city
)t2 order by business_cnt desc limit 20;

-- top ten businesses with most reviews
select review_count, name from `united-helix-310720.yelp.yelp-restaurants` 
order by review_count desc limit 10;
