#!/usr/bin/env python
# coding: utf-8

# ## Silver_Transactions
# 
# New notebook

# In[ ]:


spark.sql("""
    CREATE TABLE IF NOT EXISTS sales_project.silver.transactions (
        transaction_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        total_amount DOUBLE,
        transaction_date DATE,
        payment_method STRING,
        store_type STRING,
        order_status STRING, 
        last_updated TIMESTAMP 
    )
""")


# In[ ]:


last_processed_df = spark.sql('SELECT MAX(last_updated) as last_processed FROM sales_project.silver.transactions')
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
    last_processed_timestamp = '1900-01-01T00:00:00.000+00:00'


# In[ ]:


spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW bronze_incremental_transactions AS
    SELECT *
    FROM sales_project.bronze.transactions tr
    WHERE tr.ingestion_timestamp > '{last_processed_timestamp}'
""")


# In[ ]:


display(spark.sql('SELECT * FROM bronze_incremental_transactions'))


# In[ ]:


from pyspark.sql.functions import when, col, current_timestamp

df_new = spark.sql('SELECT * FROM bronze_incremental_transactions')
df_new = df_new.select(
    'transaction_id',
    'customer_id',
    'product_id',
    when(col('quantity') < 0, 0).otherwise(col('quantity')).alias('quantity'),
    when(col('total_amount') < 0, 0).otherwise(col('total_amount')).alias('total_amount'),
    col('transaction_date').cast('date'),
    'payment_method',
    'store_type',
    when((col('quantity') == 0) | (col('total_amount') == 0), 'Cancelled').otherwise('Completed').alias('order_status'),
    current_timestamp().alias('last_updated')
).filter(col('transaction_date').isNotNull() & col('customer_id').isNotNull() & col('product_id').isNotNull())


# In[ ]:


display(df_new)


# In[ ]:


from delta.tables import DeltaTable
df_transactions = DeltaTable.forName(spark, "sales_project.silver.transactions")


# In[ ]:


df_transactions.alias("target") \
    .merge(
        df_new.alias("source"),
        "target.transaction_id = source.transaction_id" 
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT *
FROM sales_project.silver.transactions


# In[ ]:




