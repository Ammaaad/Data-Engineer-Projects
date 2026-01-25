# Sales Data Modeling Project (Databricks)

## 📌 Project Overview
This project demonstrates an end-to-end **data modeling and data engineering workflow** for a Sales domain using **Databricks, PySpark, and SQL**.  
The goal of this project is to transform OLTP-style raw data into an **OLAP-optimized dimensional data model** for analytics and reporting.

---

## 🎯 Project Objectives
- Design a scalable **Star Schema** data model for sales analytics
- Convert transactional (OLTP) data into analytical (OLAP) format
- Implement **ETL pipelines** using PySpark and SQL
- Support **incremental data loading**
- Handle **Slowly Changing Dimensions (SCD Type 1 & Type 2)**

---

## 🏗️ Data Modeling Approach
- Used **Star Schema** for analytical efficiency
- Defined clear **fact table grain**
- Created **Fact and Dimension tables** to support business queries
- Applied **denormalization** to improve query performance

---

## 🧱 Data Model Components

### Fact Table
- Sales Fact (measures like sales amount, quantity, revenue)

### Dimension Tables
- Customer Dimension
- Product Dimension
- Payment Dimension
- Region Dimension

---

## 🔄 ETL & Data Engineering Concepts
- Implemented **ETL fundamentals** (Extract, Transform, Load)
- Used **incremental data loading** to process only new and changed data
- Managed historical changes using:
  - **SCD Type 1** (overwrite changes)
  - **SCD Type 2** (track historical changes with effective dates)

---

## ⚙️ Technologies Used
- Databricks
- PySpark
- SQL
- Delta Lake
- OLTP → OLAP Transformation

---

## 📊 Use Cases
- Sales performance analysis
- Historical trend analysis
- BI and dashboard reporting
- Data warehouse learning and best practices

---

## 📁 Repository Structure
- `/notebooks` – PySpark & SQL notebooks
- `/schemas` – Data model design
- `/etl` – ETL and incremental load logic
- `/docs` – Documentation and explanations

---

## ✅ Key Learnings
- Practical implementation of dimensional modeling
- Real-world handling of SCDs
- Building analytics-ready data models on Databricks
