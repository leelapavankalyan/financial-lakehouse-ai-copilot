# Financial Lakehouse & AI Copilot

An end-to-end financial data engineering and AI project built using **Databricks, PySpark, Delta Lake, Unity Catalog, and Databricks Agents**.

The project processes financial transaction data through a **Bronze → Silver → Gold** lakehouse architecture, performs data-quality validation and incremental processing, generates business KPIs, and provides a natural-language interface for querying structured financial data through a Databricks Supervisor Agent.

## Business Problem

Financial transaction data needs to be:

- Cleaned and validated before analysis
- Incrementally processed as new data arrives
- Stored in reliable and queryable tables
- Transformed into business-level KPIs
- Accessible to users without requiring them to write SQL queries

This project demonstrates how Databricks can be used to build a complete financial data pipeline and an AI-powered interface on top of the processed data.
## Architecture

Financial Transaction Data
          │
          ▼
    Bronze Layer
   Raw Delta Table
          │
          ▼
   Data Quality Checks
          │
          ▼
    Silver Layer
 Cleaned & Validated Data
          │
          ▼
   Delta MERGE
 Incremental Processing
          │
          ▼
     Gold Layer
   Business KPIs
          │
          ├──────────────► Databricks SQL Dashboard
          │
          ▼
   Unity Catalog
          │
          ▼
  Supervisor Agent
          │
          ▼
Natural Language Queries\

**Technology Stack**
Databricks
PySpark
Delta Lake
Unity Catalog
Databricks Jobs
Databricks SQL
Supervisor Agent
