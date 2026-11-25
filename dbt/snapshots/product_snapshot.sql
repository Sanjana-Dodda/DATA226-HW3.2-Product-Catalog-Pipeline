{% snapshot product_snapshot %}
{{ config(target_schema='SNAPSHOTS', unique_key='product_id', strategy='timestamp', updated_at='updated_at') }}
select * from {{ ref('stg_products') }}
{% endsnapshot %}
