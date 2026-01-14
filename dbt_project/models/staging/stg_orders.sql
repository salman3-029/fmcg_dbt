WITH source AS (
    SELECT * FROM {{ source('fmcg_bronze', 'orders') }}
),

filtered_data AS (
    -- 1. Keep only rows where order_qty is present
    SELECT * FROM source 
    WHERE order_qty IS NOT NULL
),

cleaned_fields AS (
    SELECT
        order_id,
        product_id,
        order_qty,
        read_timestamp,
        file_name,
        
        -- 2. Clean customer_id -> keep numeric, else set to 999999
        CASE 
            WHEN CAST(customer_id AS STRING) REGEXP '^[0-9]+$' 
            THEN CAST(customer_id AS STRING)
            ELSE '999999' 
        END AS customer_id,

        -- 3. Remove weekday name from the date text
        -- "Tuesday, July 01, 2025" -> "July 01, 2025"
        regexp_replace(order_placement_date, '^[A-Za-z]+,\\s*', '') AS order_date_raw,

        -- 6. Convert product id to string
        CAST(product_id AS STRING) AS product_id_str
    FROM filtered_data
),

parsed_dates AS (
    SELECT
        order_id,
        customer_id,
        product_id_str AS product_id,
        order_qty,
        read_timestamp,
        file_name,
        -- 4. Parse order_placement_date using multiple possible formats
        COALESCE(
            TRY_TO_DATE(order_date_raw, 'yyyy/MM/dd'),
            TRY_TO_DATE(order_date_raw, 'dd-MM-yyyy'),
            TRY_TO_DATE(order_date_raw, 'dd/MM/yyyy'),
            TRY_TO_DATE(order_date_raw, 'MMMM dd, yyyy')
        ) AS order_placement_date
    FROM cleaned_fields
),

deduplicated AS (
    -- 5. Drop duplicates based on the 5 specific columns
    SELECT * FROM parsed_dates
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id, order_placement_date, customer_id, product_id, order_qty 
        ORDER BY read_timestamp DESC
    ) = 1
),
product_join as (
    SELECT
        a.*,
        b.product_code
    FROM deduplicated a
    INNER JOIN {{ ref('stg_products') }} b
    ON a.product_id = b.product_id
)

SELECT * FROM product_join