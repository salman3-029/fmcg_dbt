{{ config(
    materialized = 'incremental',
    unique_key = 'product_code',
    incremental_strategy = 'merge'
) }}

with source as (
    /* In your notebook, the gold layer selects these specific 
       columns from the silver table.
    */
    select 
        product_code,
        product_id,
        division,
        category,
        product,
        variant
    from {{ ref('stg_products') }}

    /* If you have a master product seed file (similar to dim_customers_seed), 
       it is added here during the initial run.
    */
    {% if not is_incremental() %}
    -- Optional: Uncomment if you have a product seed file
    
    union all

    select 
        product_code,
        product_id,
        division,
        category,
        product,
        variant
    from {{ ref('dim_products_seed') }}
    
    {% endif %}
)

select * from source