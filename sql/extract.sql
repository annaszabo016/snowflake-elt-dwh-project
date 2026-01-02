-- =============
-- extract step
-- source: marketplace dataset
-- target: staging schema
--dataset: SWORDFISH_CONSUMER_INSTORE__ECOMMERCE_RECEIPT_TRANSACTION_DATA__CPG
--table: CE_BASKETVIEW_SIGNAL_CPG_GROWTH_DATA_SAMPLE
-- =============

create or replace database dwh_sales_project;
use database dwh_sales_project;

create or replace schema staging;

create or replace table staging.sales_staging as
select * from SWORDFISH_CONSUMER_INSTORE__ECOMMERCE_RECEIPT_TRANSACTION_DATA__CPG.SAMPLE_DATA.CE_BASKETVIEW_SIGNAL_CPG_GROWTH_DATA_SAMPLE;

select count(*) as rowcount from staging.sales_staging;
select * from staging.sales_staging;
