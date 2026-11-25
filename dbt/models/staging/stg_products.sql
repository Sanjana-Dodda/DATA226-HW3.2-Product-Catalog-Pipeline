select
 trim(product_id) as product_id, trim(name) as name, trim(category) as category,
 try_to_decimal(price,10,2) as price, nullif(trim(description),'') as description,
 try_to_decimal(rating,3,2) as rating, created_at::timestamp_ntz as created_at, updated_at::timestamp_ntz as updated_at
from {{ source('raw','raw_products') }}
where product_id is not null
