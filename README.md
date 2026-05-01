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
- `/notebooks` — Databricks processing notebooks (raw→processed, processed→curated)
- `/modules` — Reusable Python helper functions (validation, anonymisation, quarantine)
- `/sql` — Synapse Serverless SQL scripts and analytical queries
- `/adf` — ADF pipeline JSON export
- `/docs/screenshots` — Evidence screenshots from Azure
- `/docs/query-results` — Exported analytical query results (CSV and PNG)

## AI Use Declaration
In preparing this project, the following AI tools were used:

Claude (Anthropic) to clarify technical concepts and check understanding of the module material.
Claude (Anthropic) to assist with producing the architecture diagram in draw.io based on my own design.
Claude (Anthropic) for assistance with code generation in the PySpark notebooks, which I tested and verified against the dataset.

I confirm that I have not used AI to generate content presented as my own work, and I remain fully responsible for the ideas, analysis, and conclusions in this project.
