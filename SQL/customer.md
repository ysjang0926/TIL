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

## 월별 거래액, 주문건단가, 각 지표의 월별 증감율
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
이건 조금더 고민해봐야할듯
* 하드코딩
```sql
with month_category_sales as (
  select format_timestamp('%Y-%m', purchase_date) as month
        ,b.name_1 as name_1st
        ,count(distinct a.customer_id) as buyers
  from sales a
  left join category b on a.category_id = b.category_id
  group by month, name_1
)
,sales_with_lag as (
  select month, name_1, buyers
        ,lag(customer_count, 1) over (partition by category_1 order by month) as customer_count_lag_1
        ,lag(customer_count, 2) over (partition by category_1 order by month) as customer_count_lag_2
        ,lag(customer_count, 3) over (partition by category_1 order by month) as customer_count_lag_3
  from month_category_sales
)
,max_last_3_months as (
  select month, name_1, buyers
        ,greatest(customer_count_lag_1, customer_count_lag_2, customer_count_lag_3) as max_customer_count
        ,case when greatest(customer_count_lag_1, customer_count_lag_2, customer_count_lag_3) = customer_count then month
              when greatest(customer_count_lag_1, customer_count_lag_2, customer_count_lag_3) = customer_count_lag_2 then lag(month, 2) over (partition by category_1 order by month)
              when greatest(customer_count_lag_1, customer_count_lag_2, customer_count_lag_3) = customer_count_lag_3 then lag(month, 3) over (partition by category_1 order by month)
              end as highest_month
  from sales_with_lag
)
select month, name_1, buyers
      ,highest_month as bf_top_month
      ,max_customer_count as bf_top_buyers
      ,(buyers - bf_top_buyers) / bf_top_buyers * 100 as growth_rate
from max_last_3_months
```

* row_number
-> 조금더 고민해보기


  
```sql
with month_category_sales as (
  select format_timestamp('%Y-%m', purchase_date) as month
        ,b.name_1 as name_1st
        ,count(distinct a.customer_id) as buyers
  from sales a
  left join category b on a.category_id = b.category_id
  group by month, name_1
)
,sales_with_lag as (
  select month, name_1st, buyers
        ,lag(month, 1) over (partition by name_1st order by month) as prev_month,
        ,lag(buyers, 1) over (partition by name_1st order by month) as prev_buyers
        ,lag(month, 2) over (partition by name_1st order by month) as prev2_month
        ,lag(buyers, 2) over (partition by name_1st order by month) as prev2_buyers
        ,lag(month, 3) over (partition by name_1st order by month) as prev3_month
        ,lag(buyers, 3) over (partition by name_1st order by month) as prev3_buyers
  from category_sales
)
,ranked_sales as (
  select month, name_1st, buyers
        ,prev_month, prev_buyers
        ,prev2_month, prev2_buyers
        ,prev3_month, prev3_buyers
        ,row_number() over (partition by month, name_1st order by buyers desc) as rn
  from (
      select month, name_1st, buyers
      from sales_with_lag
      union all
      select name_1st, prev_month as month, prev_buyers as buyers
      from sales_with_lag
      union all
      select name_1st, prev2_month as month, prev2_buyers as buyers
      from sales_with_lag
      union all
      select name_1st, prev3_month as month, prev3_buyers as buyers
      from sales_with_lag
  ) as combined_data
)
```

-- 각 카테고리별로 월별 데이터에서 고객수가 가장 많은 월을 찾습니다.
ranked_sales as (
    select
        category_1,
        month as curr_month,
        customer_count,
        prev_month,
        prev_customer_count,
        prev2_month,
        prev2_customer_count,
        prev3_month,
        prev3_customer_count,
        row_number() over (partition by category_1, month order by customer_count desc) as rn
    from (
        select 
            category_1,
            month,
            customer_count
        from flattened_data
        union all
        select
            category_1,
            prev_month as month,
            prev_customer_count as customer_count
        from flattened_data
        union all
        select
            category_1,
            prev2_month as month,
            prev2_customer_count as customer_count
        from flattened_data
        union all
        select
            category_1,
            prev3_month as month,
            prev3_customer_count as customer_count
        from flattened_data
    ) as combined_data
)
-- 각 카테고리별로 직전 3개월 중 가장 높은 고객수를 가진 월을 선택합니다.
select
    category_1,
    curr_month as highest_month,
    customer_count as highest_customer_count
from ranked_sales
where rn = 1;
