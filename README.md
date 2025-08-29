# Bike Store Sales Data Warehouse Project 🚲📊  

## 📌 Project Overview  
This project is an **end-to-end data warehouse and analytics pipeline** built using SQL Server Management Studio (SSMS), with future extensions into **Python** and **Power BI**.  

The dataset is based on a **bike store** containing information about:  
- **Sales** (orders, customers, staffs, stores)  
- **Production** (products, categories, brands, stocks)  

Our goal is to:  
1. **Design a scalable data warehouse** using medallion architecture (Bronze → Silver → Gold).  
2. **Implement ETL processes** to clean, transform, and integrate raw sales & production data.  
3. **Enable reporting and analytics** through Python (data analysis) and Power BI (dashboards).  
4. Provide insights such as:  
   - Top-selling products and categories  
   - Sales trends over time  
   - Store and staff performance  
   - Inventory management  

---

## 📂 Dataset  
The dataset used in this project comes from Kaggle:  
[Bike Store Sales Dataset](https://www.kaggle.com/datasets/dillonmyrick/bike-store-sample-database/)

---

## 🏗️ High-Level Architecture  
Below is the high-level architecture diagram showing the **data flow**.  

![Architecture Diagram](Docs/High%20Level%20Architecture.png)

---

## 🛠️ Tools & Technologies  
- **SQL Server (SSMS)** → schema creation, ETL, procedures  
- **Python (pandas, numpy, matplotlib/seaborn/plotly)** → advanced analysis (planned)  
- **Power BI** → interactive dashboards (planned)  
- **GitHub** → version control & collaboration  

---

## 📊 Current Progress  
✅ Uploaded raw dataset into repo  
✅ ERD diagram for database schema  
✅ Repository structure created  
✅ Created High level architecture diagram   
✅ Created data base and schemas   
✅ Created ddl bronze and procedure to load data in bronze layer.   
✅ Did quality check on bronze layer and made required changes and conversions.   
✅ Created ddl silver and procedure to load data in silver layer from bronze with all data quality checks, required conversions and derived columns for extra metrics.   
✅ Created data model(star schema) diagram for gold layer.                                      
✅ Created ddl gold and procedure to load data from silver to gold in the form of fact and dim tables.

🔄 Next: Work on indexing, create views and answer some business questions.

---
