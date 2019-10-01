-- This query generates a row for every date between 2016-01-01 and 2025-01-01

with
  numbers_table as (
    select
      p0.n
      + p1.n*2
      + p2.n * POWER(2,2)
      + p3.n * POWER(2,3)
      + p4.n * POWER(2,4)
      + p5.n * POWER(2,5)
      + p6.n * POWER(2,6)
      + p7.n * POWER(2,7)
      + p8.n * POWER(2,8)
      + p9.n * POWER(2,9)
      + p10.n * POWER(2,10)
     + p11.n * POWER(2,11)
      as n

    from
      (SELECT 0 as n UNION SELECT 1) p0,
      (SELECT 0 as n UNION SELECT 1) p1,
      (SELECT 0 as n UNION SELECT 1) p2,
      (SELECT 0 as n UNION SELECT 1) p3,
      (SELECT 0 as n UNION SELECT 1) p4,
      (SELECT 0 as n UNION SELECT 1) p5,
      (SELECT 0 as n UNION SELECT 1) p6,
      (SELECT 0 as n UNION SELECT 1) p7,
      (SELECT 0 as n UNION SELECT 1) p8,
      (SELECT 0 as n UNION SELECT 1) p9,
      (SELECT 0 as n UNION SELECT 1) p10,
      (SELECT 0 as n UNION SELECT 1) p11
  ),

  date_table as (
    select
        ('2025-01-01'::date - row_number() over (order by true))::date as series
    from numbers_table
    order by series asc
  )


select
  series as series_date
  , date_trunc('month', series) as series_month
  , date_trunc('week', series) as series_week

from date_table
where series >= '2016-01-01'