# 🚀 Azure Databricks End-to-End Data Engineering Project

![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Azure Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00B3A4?style=for-the-badge&logo=delta&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

## DataFlows & Workflows
<img width="1134" height="211" alt="image" src="https://github.com/user-attachments/assets/c9857073-6047-40eb-8d4a-076b184537bf" />
<img width="1527" height="386" alt="image" src="https://github.com/user-attachments/assets/d89cf5ee-e5c2-41ba-8e04-229a65fa38ff" />


# 🚀 Azure Databricks End-to-End Data Engineering Project

This project demonstrates a **real-time end-to-end Data Engineering pipeline** built using **Azure Databricks**, **Delta Lake**, and **Apache Spark**.  

It follows **Medallion Architecture (Bronze → Silver → Gold)** and implements **incremental ingestion, transformations, dimensional modeling, and SCD handling**.

Designed as a **portfolio project** to simulate production-grade data pipelines.

---

## 📌 Architecture Overview

Bronze Layer  
→ Raw ingestion using **Databricks AutoLoader**

Silver Layer  
→ Cleansing, transformations, deduplication, enrichment

Gold Layer  
→ Star schema modeling with Fact & Dimension tables  
→ SCD Type 1 & Type 2 implemented using Delta Live Tables

---

## 🛠 Tech Stack

- Azure Data Lake Storage Gen2  
- Azure Databricks  
- Delta Lake  
- Apache Spark (PySpark)  
- Unity Catalog  
- Delta Live Tables (DLT)  
- Azure Data Factory  
- GitHub  

---

## ✨ Key Features

### ✅ Medallion Architecture
Structured Bronze, Silver, and Gold layers.

---

### ✅ Incremental Data Loading
- Spark Structured Streaming  
- Databricks AutoLoader  
- Exactly-once ingestion

---

### ✅ PySpark + OOP
Reusable transformation logic using Python classes.

---

### ✅ Unity Catalog
Centralized metadata:
- External locations  
- Schemas  
- Tables  

---

### ✅ Slowly Changing Dimensions

- SCD Type 1  
- SCD Type 2 using Delta Live Tables CDC

---

### ✅ Dimensional Modeling
Star schema with:

- Fact Tables  
- Dimension Tables  
- Surrogate Keys  

---

### ✅ Delta Live Tables
Automated pipelines:
- Expectations
- CDC
- Streaming ingestion

---

### ✅ ETL Workflows
End-to-end orchestration using:

- Azure Data Factory  
- Databricks notebooks  
- Lookup + ForEach pipelines  

---

## 📂 Project Structure
```
databricks_notebooks/
│
├── 1_AutoLoader.ipynb
├── 2_silver.ipynb
├── 3_lookup.ipynb
├── 4_silver.ipynb
├── 5_LookUpNotebook.ipynb
├── 6_GetDayNumber.ipynb
├── 7_DLT_Notebook.ipynb
│
factory/
dataset/
pipeline/
linkedService/
README.md
```

---

## 🧪 Data Flow

1. Source files land in ADLS  
2. AutoLoader ingests into Bronze  
3. PySpark transforms to Silver  
4. DLT applies SCD logic  
5. Gold tables created for analytics  
6. ADF orchestrates pipelines  

---

## 📊 Gold Layer Tables

- dimuser  
- dimtrack  
- dimdate  
- factstream  

---

## 🎯 Learning Outcomes

- Real-time streaming ingestion  
- Delta Lake operations  
- CDC pipelines  
- Star schema design  
- Databricks + Azure integration  
- Production-style ETL workflows  

---

## 👨‍💻 Author

**Rohan Dubey**

Aspiring Data Engineer | Azure | Databricks | PySpark  

---

## ⭐ If you like this project

Give it a star ⭐ and feel free to fork!




