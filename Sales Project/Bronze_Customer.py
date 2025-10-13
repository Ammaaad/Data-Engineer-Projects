#!/usr/bin/env python
# coding: utf-8

# ## Bronze_Customer
# 
# New notebook

# In[ ]:


df = spark.read.format("csv")\
        .option("header", True)\
        .option("inferSchema", True)\
        .load("/Volumes/sales_project/bronze/customer/customer.csv")

display(df)


# In[ ]:


from pyspark.sql.functions import current_timestamp

df = df.withColumn('ingestion_timestamp', current_timestamp())
display(df)


# In[ ]:


df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
    "sales_project.bronze.customer"
)


# In[ ]:


get_ipython().run_line_magic('sql', '')
select * from sales_project.bronze.customer


# In[ ]:




