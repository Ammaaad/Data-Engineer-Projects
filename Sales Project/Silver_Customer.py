#!/usr/bin/env python
# coding: utf-8

# ## Silver_Customer
# 
# New notebook

# In[ ]:


spark.sql("""
    CREATE TABLE IF NOT EXISTS sales_project.silver.customer (
        customer_id STRING,
        name STRING,
        email STRING,
        country STRING,
        customer_type STRING,
        registration_date DATE,
        age INT,
        gender STRING,
        total_purchases INT,
        customer_segment STRING,
        days_since_registration INT,
        last_updated TIMESTAMP
    )
""")


# In[ ]:


last_processed_df = spark.sql("SELECT MAX(last_updated) as last_processed FROM sales_project.silver.customer")
last_processed_timestamp = last_processed_df.collect()[0]["last_processed"]


if last_processed_timestamp is None:
    last_processed_timestamp = '1900-01-01T00:00:00.000+00:00'


# In[ ]:


spark.sql(
    f"""
    CREATE OR REPLACE TEMPORARY VIEW bronze_incremental AS
    SELECT *
    FROM sales_project.bronze.customer c
    WHERE c.ingestion_timestamp > '{last_processed_timestamp}'
    """
)


# In[ ]:


display(spark.sql('SELECT * FROM bronze_incremental'))


# In[ ]:


# Create or replace a temporary view
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW silver_incremental AS
    SELECT 
        customer_id,
        name, 
        email,
        country,
        customer_type,
        registration_date,
        age,
        gender,
        total_purchases,
        CASE 
            WHEN total_purchases > 1000 THEN 'High Value' 
            WHEN total_purchases > 500 THEN 'Medium Value'
            ELSE 'Low Value' 
        END AS customer_segment,
        DATEDIFF(CURRENT_DATE(), registration_date) AS days_since_registration,
        CURRENT_TIMESTAMP() AS last_updated
    FROM bronze_incremental
    WHERE email IS NOT NULL 
      AND age BETWEEN 18 AND 100 
      AND total_purchases >= 0
""")


# In[ ]:


display(spark.sql('SELECT * FROM silver_incremental'))


# In[ ]:


spark.sql("""
    MERGE INTO sales_project.silver.customer target
    USING silver_incremental source
    ON target.customer_id = source.customer_id          
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")


# In[ ]:


display(spark.sql("SELECT * FROM sales_project.silver.customer"))


# In[ ]:




