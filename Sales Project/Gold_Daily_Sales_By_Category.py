#!/usr/bin/env python
# coding: utf-8

# ## Gold_Daily_Sales_By_Category
# 
# New notebook

# In[ ]:


spark.sql("""
    CREATE OR REPLACE TABLE sales_project.gold.category_sales AS
    SELECT p.category AS product_category,
           SUM(t.total_amount) AS category_total_sales
    FROM sales_project.silver.products p
    INNER JOIN sales_project.silver.transactions t ON p.product_id = t.product_id
    GROUP BY p.category
""")


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT *
FROM sales_project.gold.category_sales


# In[ ]:




