select count(*) raw_rows from PRODUCT_CATALOG.STAGING.RAW_PRODUCTS;
select * from PRODUCT_CATALOG.ANALYTICS.DIM_PRODUCTS order by product_id, valid_from;
select product_id,name,price,dbt_valid_from,dbt_valid_to from PRODUCT_CATALOG.SNAPSHOTS.PRODUCT_SNAPSHOT order by product_id,dbt_valid_from;
show databases like '%PRODUCT_CATALOG%';
