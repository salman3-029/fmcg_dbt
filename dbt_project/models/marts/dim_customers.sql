-- set the incremental dbt configuration for this model
{{ config(
    materialized = 'incremental',
    unique_key = 'customer_code',
    incremental_strategy = 'merge'
) }}
with source as (
    select 
    customer_code,
    customer,
    market,
    platform,
    channel
    from {{ ref('stg_customers') }}

    {% if not is_incremental() %}

    union all

    select 
        customer_code,
        customer,
        market,
        platform,
        channel
    from {{ ref('dim_customers_seed') }}

    {% endif %}
)
select * from source