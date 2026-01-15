{{ config(
    materialized = 'incremental',
    unique_key = ['date', 'product_code', 'customer_code'],
    incremental_strategy = 'merge'
) }}

with orders_stg as (
    select 
        date_trunc('MM', order_placement_date) as date,
        customer_id as customer_code,
        product_code,
        sum(order_qty) as sold_quantity
    from {{ ref('stg_orders') }}
    group by 1,2,3

    -- Removed incremental filter: processing full staging table for merge
),
transformed_orders as (
    select 
        o.date,
        o.product_code,
        o.customer_code,
        o.sold_quantity
    from orders_stg o
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