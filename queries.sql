select avg(price) from stock where DATE(event_time)='2023-04-24' and symbol='RHT';
select symbol,MIN(price),MAX(price),AVG(price) from stock where DATE(event_time)='2023-04-24' group by symbol;



-- Show daily min, max, avg, opening price, closing price, and difference in percentage between opening and closing
WITH daily_prices AS (
    SELECT
        symbol,
        date_trunc('day', event_time) AS day,
        price,
        FIRST_VALUE(price) OVER (PARTITION BY symbol, date_trunc('day', event_time) ORDER BY event_time RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_price_of_day,
        LAST_VALUE(price) OVER (PARTITION BY symbol, date_trunc('day', event_time) ORDER BY event_time RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_price_of_day
    FROM
        stock
)
SELECT
    symbol,
    day,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    AVG(price) AS avg_price,
    MAX(first_price_of_day) AS first_price_of_day,
    MAX(last_price_of_day) AS last_price_of_day,
    MAX(((last_price_of_day - first_price_of_day)/first_price_of_day) * 100) as difference

FROM
    daily_prices
GROUP BY
    symbol,
    day
ORDER BY
    symbol,
    day;



-- Show hourly min, max, avg, initial price, last price, and difference in percentage between initial and last price
WITH hourly_prices AS (
    SELECT
        symbol,
        date_trunc('hour', event_time) AS day,
        price,
        FIRST_VALUE(price) OVER (PARTITION BY symbol, date_trunc('hour', event_time) ORDER BY event_time RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_price_of_hour,
        LAST_VALUE(price) OVER (PARTITION BY symbol, date_trunc('hour', event_time) ORDER BY event_time RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_price_of_hour
    FROM
        stock
)
SELECT
    symbol,
    day,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    AVG(price) AS avg_price,
    MAX(first_price_of_hour) AS first_price_of_hour,
    MAX(last_price_of_hour) AS last_price_of_hour,
    MAX(((last_price_of_hour - first_price_of_hour)/first_price_of_hour) * 100) as difference

FROM
    hourly_prices
GROUP BY
    symbol,
    day
ORDER BY
    symbol,
    day;



-- Show stats for the last hour
SELECT
     symbol,
     MIN(price) AS min_price,
     MAX(price) AS max_price,
     AVG(price) AS avg_price
   FROM
        stock
   WHERE
       event_time >= (CURRENT_TIMESTAMP - INTERVAL '10 minutes')
    GROUP BY symbol;