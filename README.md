# 7DATA002W CW2 — Azure Big Data Platform

## Overview
End-to-end Azure data platform built on the Amazon Fine Food Reviews dataset (568,454 rows).

## Architecture
- **Ingestion:** Azure Data Factory (ADF) Copy Activity
- **Storage:** Azure Data Lake Storage Gen2 (raw / processed / curated / quarantine)
- **Processing:** Azure Databricks (PySpark)
- **Serving:** Azure Synapse Analytics Serverless SQL
- **Security:** Azure Key Vault, SHA-256 hashing, RBAC via Microsoft Entra ID

## Dataset
Amazon Fine Food Reviews — Kaggle (snap/amazon-fine-food-reviews)
568,454 rows, 10 columns: Id, ProductId, UserId, ProfileName, HelpfulnessNumerator, HelpfulnessDenominator, Score, Time, Summary, Text

## Repository Structure
- `/notebooks` — Databricks processing notebooks
- `/modules` — Reusable Python helper modules
- `/sql` — Synapse SQL external tables and analytical queries
- `/adf` — ADF pipeline JSON exports
- `/docs` — Architecture diagram and screenshots

## AI Acknowledgement
Claude (Anthropic) was used for scaffolding and code refinement. All logic was reviewed and adapted manually.
