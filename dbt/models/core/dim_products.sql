{{ config(materialized='table') }}
select
 md5(product_id || '|' || dbt_valid_from::varchar) as product_sk, product_id, name, category, price, description, rating,
 created_at, updated_at, dbt_valid_from as valid_from, dbt_valid_to as valid_to,
 iff(dbt_valid_to is null,true,false) as is_current
from {{ ref('product_snapshot') }}
