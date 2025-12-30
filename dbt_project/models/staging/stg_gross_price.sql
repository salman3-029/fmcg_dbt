WITH source AS (
    SELECT * FROM {{ source('fmcg_bronze', 'gross_price') }}
),

normalized_dates AS (
    SELECT 
        *,
        -- Replicating F.coalesce(F.try_to_date(...)) logic
        COALESCE(
            TRY_TO_DATE(month, 'yyyy/MM/dd'),
            TRY_TO_DATE(month, 'dd/MM/yyyy'),
            TRY_TO_DATE(month, 'yyyy-MM-dd'),
            TRY_TO_DATE(month, 'dd-MM-yyyy')
        ) AS month_parsed
    FROM source
),

cleaned_prices AS (
    SELECT 
        product_id,
        month_parsed AS month,
        read_timestamp,
        file_name,
        file_size,
        -- Validating numeric string, fixing negatives, casting to DOUBLE
        CASE 
            WHEN month_parsed IS NULL THEN 0 -- Safety check for unparseable dates
            WHEN CAST(gross_price AS STRING) REGEXP '^-?[0-9]+(\.[0-9]+)?$' THEN 
                ABS(CAST(gross_price AS DOUBLE))
            ELSE 0 
        END AS gross_price
    FROM normalized_dates
)

SELECT 
    p.product_code, -- Fetched from Silver products table
    c.product_id,
    c.month,
    c.gross_price,
    c.read_timestamp,
    c.file_name,
    c.file_size
FROM cleaned_prices c
INNER JOIN {{ ref('stg_products') }} p 
    ON c.product_id = p.product_id