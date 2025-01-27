# ETL Project for Multi-Specialty Hospital Chain
Overview
This project implements an ETL (Extract, Transform, Load) pipeline to manage customer data for a multi-specialty hospital chain. The project consists of two ETL stages:

ETL Stage 1:
Load raw customer data from files into a PostgreSQL staging table.

Handles file ingestion.
Applies basic data validation.
Creates the staging table with partitions and indexes for optimized querying.
ETL Stage 2:
Move validated data from the staging table to a final partitioned table.

Divides data by country using dynamic partitions.
Performs advanced data transformations (e.g., calculating age, days since last consultation).
Handles country-wise partition creation dynamically.
