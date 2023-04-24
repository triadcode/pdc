select avg(price) from stock where DATE(event_time)='2023-04-24' and symbol='RHT';
select symbol,MIN(price),MAX(price),AVG(price) from stock where DATE(event_time)='2023-04-24' group by symbol;