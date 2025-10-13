#!/usr/bin/env python
# coding: utf-8

# ## Gold_Daily_Sales
# 
# New notebook

# In[ ]:


spark.sql("""
    CREATE OR REPLACE TABLE sales_project.gold.daily_sales AS
    SELECT transaction_date,
           SUM(total_amount) AS daily_total_sales
    FROM sales_project.silver.transactions
    GROUP BY transaction_date
""")


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT *
FROM sales_project.gold.daily_sales


# In[ ]:




