# Data Warehouse Project using SQL Server
Building Data Warehouse with SQL Server, ETL processes, data modeling and analytics


### Data Architecture
![Architecture](./docs/High%20Level%20Architecture.png)

* **Bronze Layer**: Stores the raw, unprocessed data as is from the source system (CSV files) into SQL Server
* **Silver Layer**: This layer will store the cleaned, normalized and standardized data. 
* **Gold Layer**: Stores the Business ready data which is modeled into a star schema for reporting and analysis


This project is to build a data warehouse using the Medallion architecture.
This includes
* Data Architecture: Uses Medallion architecure (Bronze, Silver, Gold Layers)
* ETL Pipelines: Extract the data from the Source Systems and then Transform and Load the data into warehouse
* Data Modeling: Developing dimensions and fact tables for analytics



