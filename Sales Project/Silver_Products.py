#!/usr/bin/env python
# coding: utf-8

# ## Silver_Products
# 
# New notebook

# In[ ]:


spark.sql("""
    CREATE TABLE IF NOT EXISTS sales_project.silver.products (
        product_id STRING,
        name STRING,
        category STRING,
        brand STRING,
        price DOUBLE,
        stock_quantity INT,
        rating DOUBLE,
        is_active BOOLEAN,
        price_category STRING,
        stock_status STRING,
        last_updated TIMESTAMP
    )
""")


# In[ ]:


last_processed_df = spark.sql("SELECT MAX(last_updated) as last_processed FROM sales_project.silver.products")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
    last_processed_timestamp = '1900-01-01T00:00:00.000+00:00'


# In[ ]:


spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW bronze_incremental_products AS
    SELECT *
    FROM sales_project.bronze.products products 
    WHERE products.ingestion_timestamp > '{last_processed_timestamp}'
""")


# In[ ]:


display(spark.sql('SELECT * FROM bronze_incremental_products'))


# In[ ]:


spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW silver_incremental_products AS
    SELECT product_id,
           name,
           brand,
           category,
           is_active,
           CASE WHEN price < 0 THEN 0 
                ELSE price 
           END AS price,
           CASE WHEN rating < 0 THEN 0 
                WHEN rating > 5 THEN 5
                ELSE rating
            END AS rating,
           CASE WHEN stock_quantity < 0 THEN 0 
                ELSE stock_quantity 
           END AS stock_quantity,
           CASE WHEN price > 1000 THEN 'Premium'
                WHEN price > 100 THEN 'Standard'
                ELSE 'Budget'
           END AS price_category,
           CASE WHEN stock_quantity = 0 THEN 'Out of Stock'
                WHEN stock_quantity < 100 THEN 'Low Stock'
                WHEN stock_quantity < 500 THEN 'Moderate Stock'
                ELSE 'Sufficient Stock'
           END AS stock_status,
           CURRENT_TIMESTAMP() AS last_updated
    FROM bronze_incremental_products
    WHERE name IS NOT NULL AND category IS NOT NULL
         
""")


# In[ ]:


display(spark.sql('SELECT * FROM silver_incremental_products'))


# In[ ]:


spark.sql("""
    MERGE INTO sales_project.silver.products target
    USING silver_incremental_products source
    ON target.product_id = source.product_id          
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")


# In[ ]:


display(spark.sql('SELECT * FROM sales_project.silver.products'))


# In[ ]:




