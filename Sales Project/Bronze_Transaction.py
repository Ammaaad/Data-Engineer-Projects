#!/usr/bin/env python
# coding: utf-8

# ## Bronze_Transaction
# 
# New notebook

# In[ ]:


df = spark.read.parquet("/Volumes/sales_project/bronze/transaction/transaction.snappy.parquet")

from pyspark.sql.functions import to_timestamp

df = df.withColumn('transaction_date', to_timestamp('transaction_date'))


display(df)


# In[ ]:


from pyspark.sql.functions import current_timestamp

df = df.withColumn('ingestion_timestamp', current_timestamp())


display(df)


# In[ ]:


df.write.format("delta").mode("append").saveAsTable("sales_project.bronze.transactions")


# In[ ]:


get_ipython().run_line_magic('sql', '')
select * from sales_project.bronze.transactions


# In[ ]:




