문제는 크게 난이도 높지 않았지만, 내가 요즘 쿼리를 짤 때 chatgpt에 의존도가 높았다는 사실을 깨닫게 되는 순간이었다. <br>
반성하는 시간을 가지며 쿼리 복기를 다시 해본다.

## 첫구매까지 소요되는 일자의 전체 평균
```sql
with first_purchase as (
  select customer_id, min(purchase_date) as first_purchase_date
  from sales
  group by customer_id
)
,first_login as (
  select customer_id, min(login_date) as first_login_date
  from sales
  group by customer_id
)
select avg(days_to_first_purchase) as avg_days_to_first_purchase
from (
  select a.customer_id, date_diff(a.first_purchase_date, b.first_login_date, day) as days_to_first_purchase
  from first_purchase a
  left join first_login b on a.customer_id = b.customer_id
)
```

## 구매금액대별로 구매자 수, 구매금액, 구매금액의 비중
```sql
with sales_summary as (
  select customer_id, sum(sales_price) as total_spent
  from sales
  group by customer_id
)
,spent_range as (
  select customer_id, total_spent
  ,case when total_spent between 0 and 10000 then '0-10000'
        when total_spent between 10001 and 50000 then '10001-50000'
        when total_spent between 50001 and 100000 then '50001-100000'
        when total_spent > 100000 then '>100000'
        else 'null' end as spending_range
  from sales_summary
)
select spending_range
      ,count(customer_id) as buyers
      ,sum(total_spent) as gmv
      ,sum(total_spent) / (select sum(total_spent) from sales_summary) * 100 as gmv_prop
from spending_brackets
group by spending_range
```

## 월별 거래액(GMV), 주문건단가(AOV), 각 지표의 월별(MoM) 증감율
```sql
with monthly_sales as (
  select format_timestamp('%Y-%m', purchase_date) as month
        ,sum(sales_price) as gmv
        ,count(order_id) as order_count
        ,sum(sales_price) / count(order_id) as aov
  from sales
  group by month
)
select month, gmv, aov
      ,(gmv - lag(gmv) over (order by month)) / lab(gmv) over (order by month) * 100 as gmv_growth_rate
      ,(aov - lag(aov) over (order by month)) / lab(aov) over (order by month) * 100 as aov_growth_rate
from monthly_sales
```

## 월별 1차 카테고리별 구매고객수 + 해당 카테고리의 직전 3개월 내에서 구매고객수가 최고인 월,구매고객수,증감률
```sql
with category_saels as (
select format_timestamp('%Y-%m', purchase_date) as month
      ,b.name_1 as name_1st
      ,count(distinct a.customer_id) as buyers
from sales a
left join category b on a.category_id = b.category_id
group by month, name_1st
)
,highest_customers as (
select
)
```



category_growth AS (
    SELECT
        month,
        category_1,
        customer_count,
        LAG(customer_count, 1) OVER (PARTITION BY category_1 ORDER BY month) AS prev_month_count,
        (customer_count - LAG(customer_count, 1) OVER (PARTITION BY category_1 ORDER BY month)) / LAG(customer_count, 1) OVER (PARTITION BY category_1 ORDER BY month) * 100 AS MoM_growth
    FROM category_sales
),
highest_customers AS (
    SELECT
        category_1,
        MAX(customer_count) AS highest_customer_count,
        month AS highest_month
    FROM category_sales
    GROUP BY category_1
)
SELECT 
    cg.*,
    hc.highest_month,
    hc.highest_customer_count
FROM category_growth cg
JOIN highest_customers hc ON cg.category_1 = hc.category_1 AND cg.customer_count = hc.highest_customer_count;
