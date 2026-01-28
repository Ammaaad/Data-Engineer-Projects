#!/usr/bin/env python
# coding: utf-8

# ## Silver Notebook
# 
# null

# 

# ### **UTILITIES**

# In[1]:


notebookutils.fs.ls("Files")


# #### **Data Reading**

# ##### **Same LakeHouse**

# In[2]:


df = spark.read.format("csv")\
    .option("Header", True)\
    .option("inferSchema", True)\
    .load("Files/Customer Data Shortcut")


# In[3]:


display(df)


# ### **Bronze LakeHouse**

# #### **Customer Data**

# In[26]:


df_cust = spark.read.table("Bronze_LH.dbo.customer_tbl")


# In[3]:


display(df_cust)


# In[6]:


df_cust = spark.read.format("delta")\
    .load("abfss://AmmadWS@onelake.dfs.fabric.microsoft.com/Bronze_LH.Lakehouse/Tables/dbo/customer_tbl")


# In[7]:


display(df_cust)


# In[10]:


from pyspark.sql.functions import *
from pyspark.sql.types import *


# In[6]:


df_cust = df_cust.withColumn("first_name",split(col("name")," ")[0])\
        .withColumn("last_name",split(col("name")," ")[1])

display(df_cust)


# In[7]:


df_cust = df_cust.fillna({"last_name":"NA"})

display(df_cust)


# In[12]:


from pyspark.sql.functions import col, regexp_replace, trim

df_cust = df_cust.withColumn(
    "last_name",
    trim(
        regexp_replace(
            regexp_replace(col("last_name"), "@", ""),   # remove @
            "\\s+", " "                                  # multiple spaces → single
        )
    )
)

display(df_cust)


# In[27]:


df_cust.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("Silver_LH.dbo.customer_tbl")


# ### **Fact Table**

# In[1]:


df_fact = spark.read.table("Bronze_LH.dbo.fact_tbl")


# In[4]:


display(df_fact)


# In[6]:


df_fact.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("Silver_LH.dbo.fact_tbl")


# #### **Store**

# In[7]:


df_store = spark.read.table("Bronze_LH.dbo.store_tbl")


# In[8]:


display(df_store)


# In[17]:


df_store = df_store.withColumn("address",concat(col("district"),lit("-"),col("upazila")))
df_store = df_store.drop("district","upazila")

display(df_store)


# In[18]:


df_store.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("Silver_LH.dbo.store_tbl")


# #### **Trans**

# In[21]:


df_trans = spark.read.table("Bronze_LH.dbo.trans_tbl")

display(df_trans)


# In[22]:


df_trans = df_trans.withColumn("bank_name",regexp_replace(col("bank_name"),"None","Not Available"))

display(df_trans)


# In[23]:


df_trans.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("Silver_LH.dbo.trans_tbl")


# In[ ]:




