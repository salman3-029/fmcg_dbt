{{ config(
    materialized = 'incremental',
    unique_key = ['date', 'product_code', 'customer_code'],
    incremental_strategy = 'merge'
) }}

with orders_stg as (
    select 
        order_placement_date as date,
        customer_id as customer_code,
        product_id,
        order_qty as sold_quantity
    from {{ ref('stg_orders') }}
    -- Removed incremental filter: processing full staging table for merge
),

products_stg as (
    select 
        product_id, 
        product_code 
    from {{ ref('stg_products') }}
),

transformed_orders as (
    select 
        o.date,
        p.product_code,
        o.customer_code,
        o.sold_quantity
    from orders_stg o
    inner join products_stg p 
        on o.product_id = p.product_id
),

final_source as (
    select 
        date,
        product_code,
        customer_code,
        sold_quantity
    from transformed_orders

    {% if not is_incremental() %}
    union all

    -- Load from seed file during full refresh
    select 
        date,
        product_code,
        customer_code,
        sold_quantity
    from {{ ref('fact_orders_seed') }}
    {% endif %}
)

select 
    date,
    product_code,
    customer_code,
    sold_quantity
from final_source