with source as (
    select 
    customer_id,
    trim(customer_name) as customer_name,
    city,
    read_timestamp,
    file_name,
    file_size
    from {{ source('fmcg_bronze', 'customers') }}
),
deduped as (
    select 
    customer_id,
    customer_name,
    city,
    read_timestamp,
    file_name,
    file_size,
    row_number() over (
        partition by customer_id 
        order by read_timestamp desc
    ) as rn
    from source
    qualify rn = 1   
),
city_cleaned as (
    select
        customer_id,
        customer_name,
        read_timestamp,
        file_name,
        file_size,
        case
            when lower(city) in ('bengaluruu', 'banglore', 'bangalore') then 'Bengaluru'
            when lower(city) in ('hyderabadd', 'hyderbad') then 'Hyderabad'
            when lower(city) in ('newdelhi', 'newdheli', 'newdelhee') then 'New Delhi'
            else city
        end as city
    from deduped
),
customer_cleaned as (
select 
    customer_id,
    case when customer_name is null then null
         else initcap(customer_name) 
    end as customer_name,
    read_timestamp,
    file_name,
    file_size,
    case
        when city in ('Bengaluru', 'Hyderabad', 'New Delhi') then city
        else 'Unknown'
    end as city
from city_cleaned
), city_fix as (
    select 
        789101 as customer_id,
        'Bengaluru' as fixed_city
    union all
    select 
        789521 as customer_id,
        'Hyderabad' as fixed_city
    union all
    select 
        789603 as customer_id,
        'Hyderabad' as fixed_city
    union all
    select 
        789403 as customer_id,
        'New Delhi' as fixed_city
    union all
    select 
        789420 as customer_id,
        'Bengaluru' as fixed_city

), 
extra_columns as (
select 
    a.customer_id::string as customer_code,
    customer_name,
    coalesce(b.fixed_city,a.city) as city,
    customer_name || ' - ' || coalesce(b.fixed_city,a.city) as customer,
    'India' as market,
    'Sports Bar' as platform,
    'Acquired' as channel
from customer_cleaned a
left join city_fix b
on a.customer_id = b.customer_id
)
select * from extra_columns