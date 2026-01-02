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
    row_number() over (order by PRODUCT_SYMBOL, PRODUCT_BRAND_NAME, CATEGORY_NAME) AS dim_productid,
    PRODUCT_SYMBOL,
    PRODUCT_BRAND_NAME,
    CATEGORY_NAME,
    PRODUCT_SEGMENT_NAME,
    PRODUCT_PRIMARY_SEGMENT_FLAG
from sales_staging
group by 
    PRODUCT_SYMBOL,
    PRODUCT_BRAND_NAME,
    CATEGORY_NAME,
    PRODUCT_SEGMENT_NAME,
    PRODUCT_PRIMARY_SEGMENT_FLAG
order by PRODUCT_SYMBOL;

select * from dim_product;

create or replace table dim_geo as
select
    row_number() over (order by GEO, GEO_TYPE) as dim_geoid,
    GEO,
    GEO_TYPE
from sales_staging
group by GEO, GEO_TYPE
order by GEO;

select * from dim_geo;

create or replace table dim_merchant as
select
    row_number() over (order by MERCHANT_CHANNEL, MERCHANT_SUBINDUSTRY_NAME) as dim_merchantid,
    MERCHANT_CHANNEL,
    MERCHANT_SUBINDUSTRY_NAME
from sales_staging
group by MERCHANT_CHANNEL, MERCHANT_SUBINDUSTRY_NAME;

select * from dim_merchant;

create or replace table fact_sales as
select
    s.ITEM_COUNT,
    s.TRANS_COUNT,
    s.SPEND_AMOUNT_USD,
    d.dim_dateid,
    p.dim_productid,
    g.dim_geoid,
    m.dim_merchantid,
    -- 1. window func: napi eladasi rangsor osszeg alapjan
    rank() over (partition by d.dim_dateid order by s.SPEND_AMOUNT_USD desc) as sales_rank_daily,
    -- 2. window func: running total idorendben
    sum(s.SPEND_AMOUNT_USD) over (order by d.date rows between unbounded preceding and current row) as total_cumulative_spend
from sales_staging s
join dim_date d on cast(s.trans_date as date) = d.date
join dim_product p on s.PRODUCT_SYMBOL = p.PRODUCT_SYMBOL and s.PRODUCT_BRAND_NAME = p.PRODUCT_BRAND_NAME
join dim_geo g on s.GEO = g.GEO and s.GEO_TYPE = g.GEO_TYPE
join dim_merchant m on s.MERCHANT_CHANNEL = m.MERCHANT_CHANNEL and s.MERCHANT_SUBINDUSTRY_NAME = m.MERCHANT_SUBINDUSTRY_NAME;

select * from fact_sales;
