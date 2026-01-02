--transform step
--creating dimension tables
--dataset: SWORDFISH_CONSUMER_INSTORE__ECOMMERCE_RECEIPT_TRANSACTION_DATA__CPG

use database dwh_sales_project;
use schema staging;

create or replace table dim_date as 
select row_number() over (order by cast(trans_date as date)) as dim_dateid,
    cast(trans_date as date) as date,
    date_part(day, cast(trans_date as date)) as day,
    date_part(month, cast(trans_date as date)) as month,
    date_part(year, cast(trans_date as date)) as year,
    date_part(quarter, cast(trans_date as date)) as quarter,
    date_part(dow, cast(trans_date as date)) + 1 as day_of_week,
    case date_part(dow, cast(trans_date as date)) + 1
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
        when 7 then 'Sunday'
    end as day_of_week_name
from sales_staging
group by cast(trans_date as date)
order by date;

select * from dim_date;

create or replace table dim_product as
select
    row_number() over (order by product_symbol, product_brand_name, category_name) AS dim_productid,
    product_symbol,
    product_brand_name,
    category_name,
    product_segment_name,
    product_primary_segment_flag
from sales_staging
group by 
    product_symbol,
    product_brand_name,
    category_name,
    product_segment_name,
    product_primary_segment_flag
order by product_symbol;

select * from dim_product;

create or replace table dim_geo as
select
    row_number() over (order by GEO, GEO_TYPE) as dim_geoid,
    geo,
    geo_type
from sales_staging
group by geo, geo_type
order by geo;

select * from dim_geo;

create or replace table dim_merchant as
select
    row_number() over (order by merchant_channel, merchant_subindustry_name) as dim_merchantid,
    merchant_channel,
    merchant_subindustry_name
from sales_staging
group by merchant_channel, merchant_subindustry_name;

select * from dim_merchant;

create or replace table fact_sales as
select
    s.item_count,
    s.trans_count,
    s.spend_amount_usd,
    d.dim_dateid,
    p.dim_productid,
    g.dim_geoid,
    m.dim_merchantid,
    -- 1. window func: napi eladasi rangsor osszeg alapjan
    rank() over (partition by d.dim_dateid order by s.spend_amount_usd desc) as sales_rank_daily,
    -- 2. window func: running total idorendben
    sum(s.spend_amount_usd) over (order by d.date rows between unbounded preceding and current row) as total_cumulative_spend
from sales_staging s
join dim_date d on cast(s.trans_date as date) = d.date
join dim_product p on s.product_symbol = p.product_symbol and s.product_brand_name = p.product_brand_name
join dim_geo g on s.geo = g.geo and s.geo_type = g.geo_type
join dim_merchant m on s.merchant_channel = m.merchant_channel and s.merchant_subindustry_name = m.merchant_subindustry_name;

select * from fact_sales;
