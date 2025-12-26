import pyspark.sql.functions as F
from pyspark.sql.window import Window

def model(dbt, spark):
    dbt.config(
        enabled=False,
        materialized="table",
        submission_method="job_cluster"
        )

    df_bronze = dbt.source('fmcg_bronze', 'customers')

    # Deduplicate records to get the latest entry for each customer
    window_spec = Window.partitionBy("customer_id").orderBy(F.col("read_timestamp").desc())
    df_deduped = df_bronze.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn") == 1).drop("rn")

    city_mapping = {
        'Bengaluruu': 'Bengaluru',
        'Banglore': 'Bengaluru',
        'Bangalore': 'Bengaluru',
        'Hyderabadd': 'Hyderabad',
        'Hyderbad': 'Hyderabad',
        'NewDelhi': 'New Delhi',
        'NewDheli': 'New Delhi',
        'NewDelhee': 'New Delhi'
    }

    allowed_cities = ['Bengaluru', 'Hyderabad', 'New Delhi']

    # Apply the city cleaning logic
    df_transformed = (
        df_deduped
        .replace(to_replace=city_mapping, subset=["city"])
        .withColumn(
            "city",
            F.when(F.col("city").isNull(), None)
            .when(F.col("city").isin(allowed_cities), F.col("city"))
            .otherwise(None)
        )
    )

    return df_transformed
