{{ config(
    materialized = 'incremental',
    unique_key = 'product_code',
    incremental_strategy = 'merge'
) }}

with silver_data as (
    -- 1. Fetching cleaned data from staging
    select 
        product_code,
        month,
        gross_price,
        year(month) as year_val,
        -- Ranking Logic: 0 for non-zero (priority), 1 for zero prices
        case when gross_price = 0 then 1 else 0 end as is_zero
    from {{ ref('stg_gross_price') }}
),

ranked_prices as (
    -- 2. Applying the window function to get the latest price per product/year
    select 
        product_code,
        gross_price as price_inr,
        cast(year_val as string) as year,
        row_number() over (
            partition by product_code, year_val 
            order by is_zero asc, month desc
        ) as rnk
    from silver_data
),

transformed_source as (
    -- 3. Filtering for the top-ranked records as done in the notebook
    select 
        product_code,
        price_inr,
        year
    from ranked_prices
    where rnk = 1
),

final_source as (
    -- 4. Combining transformed data with the seed file during full refresh
    select * from transformed_source

    {% if not is_incremental() %}
    union all

    select 
        product_code,
        price_inr,
        year
    from {{ ref('dim_gross_price_seed') }}
    {% endif %}
)

select * from final_source