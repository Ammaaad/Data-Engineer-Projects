#!/usr/bin/env python
# coding: utf-8

# ## Fiber Project Notebook
# 
# New notebook

# In[3]:


from pyspark.sql.functions import *
from pyspark.sql.types import *


# ### **Silver Notebook**

# #### **Utilities**

# In[4]:


notebookutils.fs.ls("Files")


# #### **Data Reading**

# In[5]:


df = spark.read.format("csv")\
        .option("header", True)\
        .option("inferSchema", True)\
        .load("Files/Store_Data_Shortcut")


# In[6]:


display(df)


# #### - **Bronze**

# ##### **Customer Data**

# In[7]:


df_cust = spark.read.table("Bronze.customer_dim")


# In[8]:


display(df_cust)


# In[9]:


df_cust = df_cust.withColumn("first_name", split(col("name"), " ")[0])\
            .withColumn("last_name", split(col("name"), " ")[1])

display(df_cust)


# In[10]:


df_cust = df_cust.fillna({"last_name":"NA"})

display(df_cust)


# In[11]:


df_cust.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("Silver.customer_dim")


# #### **Fact Table**

# In[12]:


df_fact = spark.read.table("bronze.fact_table")

display(df_fact)


# In[13]:


df_fact = df_fact.withColumn("quantity",col("quantity").cast(IntegerType()))\
                .withColumn("unit_price",col("unit_price").cast(IntegerType()))\
                .withColumn("total_price",col("total_price").cast(IntegerType()))


# In[14]:


df_fact.printSchema()


# In[15]:


display(df_fact)


# In[16]:


df_fact.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("Silver.fact_table")


# #### **Store Dimension**

# In[17]:


df_store = spark.read.table("Bronze.store_dim")

display(df_store)


# In[18]:


df_store = df_store.withColumn("address",concat(col("district"),lit("-"),col("upazila")))
df_store = df_store.drop("district","upazila")


display(df_store)


# In[19]:


df_store.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("Silver.store_dim")


# #### **Trans Dimension**

# In[20]:


df_trans = spark.read.table("Bronze.Trans_Dim")

display(df_trans)


# In[21]:


df_trans = df_trans.withColumn("bank_name",regexp_replace(col("bank_name"),"None","Not Available"))


display(df_trans)


# In[22]:


df_trans.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("Silver.trans_dim")


# In[ ]:




