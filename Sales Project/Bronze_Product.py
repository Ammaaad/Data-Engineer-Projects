#!/usr/bin/env python
# coding: utf-8

# ## Bronze_Product
# 
# New notebook

# In[ ]:


df = spark.read.format("json").load("/Volumes/sales_project/bronze/products/products.json")

display(df)


# In[ ]:


spark.sql(
    """
    ALTER TABLE sales_project.bronze.products
    ADD COLUMNS (ingestion_timestamp TIMESTAMP)
    """
)


# In[ ]:


df_new.write.format('delta').mode('append').saveAsTable('sales_project.bronze.products')


# In[ ]:


get_ipython().run_line_magic('sql', '')
select * from sales_project.bronze.products


# In[ ]:




