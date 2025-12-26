WITH source AS (
    SELECT p.* ,
    _metadata.file_modification_time  as read_timestamp,
    _metadata.file_name as file_name,
    _metadata.file_size as file_size
    FROM fmcg.bronze.products p
),

deduplicated AS (
    -- PySpark: df.dropDuplicates(['product_id'])
    -- We use QUALIFY to keep the most recently read record if duplicates exist
    SELECT * FROM source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY read_timestamp DESC) = 1
),

cleaned_fields AS (
    SELECT 
        -- Pass through original IDs for logic, clean them later
        product_id AS original_product_id,
        read_timestamp,
        file_name,
        file_size,

        -- PySpark: Fix Spelling Mistake for `Protien` -> `Protein`
        regexp_replace(product_name, '(?i)Protien', 'Protein') AS product_name_clean,
        
        -- PySpark: Title Case fix + Spelling fix on category
        regexp_replace(initcap(category), '(?i)Protien', 'Protein') AS category_clean
    FROM deduplicated
),

transformed AS (
    SELECT 
        -- PySpark: 3. Create new column: product_code (SHA256 of CLEANED name)
        sha2(product_name_clean, 256) AS product_code,
        
        -- PySpark: 2. Clean product_id (Keep numeric, else 999999)
        CASE 
            WHEN CAST(original_product_id AS STRING) RLIKE '^[0-9]+$' 
            THEN CAST(original_product_id AS STRING)
            ELSE '999999' 
        END AS product_id,

        -- PySpark: 1. Add division column based on Cleaned Category
        CASE 
            WHEN category_clean IN ('Energy Bars', 'Protein Bars') THEN 'Nutrition Bars'
            WHEN category_clean = 'Granola & Cereals' THEN 'Breakfast Foods'
            WHEN category_clean = 'Recovery Dairy' THEN 'Dairy & Recovery'
            WHEN category_clean = 'Healthy Snacks' THEN 'Healthy Snacks'
            WHEN category_clean = 'Electrolyte Mix' THEN 'Hydration & Electrolytes'
            ELSE 'Other'
        END AS division,

        category_clean AS category,
        
        -- PySpark: Rename product_name -> product
        product_name_clean AS product,

        -- PySpark: Variant column (extract text inside parentheses)
        regexp_extract(product_name_clean, '\\((.*?)\\)', 1) AS variant

    FROM cleaned_fields
)

SELECT * FROM transformed