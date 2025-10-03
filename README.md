Pre-Acquisition Sales Data Unification Project
📌 Overview

This project unified film and music sales data from two separate SQLite databases into a cloud-hosted MySQL star-schema datamart (~160k records). The goal was to create a single, reliable view of sales performance to support acquisition readiness and enable consistent analytics.

✨ Key Features

Star Schema Design: Implemented fact and dimension tables (sales, date, country, channel, customer).

Schema Reconciliation: Validated revenue attribution, channel definitions, and country keys, reducing mismatches by 35%.

ETL Workflows: Built batch loading in R + SQL (1k rows/batch) for efficient extraction, transformation, and loading.

Automated Reporting: Used R Markdown with ggplot2 and kableExtra to cut reporting time from ~4 hours to <5 minutes.

Cloud Integration: Deployed MySQL on Aiven Cloud to demonstrate cloud database capabilities.

🛠 Tech Stack

Databases: MySQL (cloud-hosted), SQLite

Languages/Tools: SQL, R

R Packages: dplyr, tidyr, reshape2, ggplot2, kableExtra, R Markdown

📂 Project Files

Since the structure is flat, all files are in the root folder:

createStarSchema.R – star schema creation

loadAnalyticsDB.R – ETL and batch inserts

BusinessAnalysis.Rmd – automated reporting

schema_validation.sql – SQL reconciliation queries

sandbox.R – testing/EDA scripts

README.md – documentation

🚀 Getting Started

Clone the repository:

git clone https://github.com/<username>/Pre-Acquisition-Sales-Data-Unification-Project.git
cd Pre-Acquisition-Sales-Data-Unification-Project


Configure .Renviron or .env with your database connection:

DB_HOST=<host>
DB_PORT=<port>
DB_NAME=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>


Run createStarSchema.R to set up the schema.

Run loadAnalyticsDB.R to load sales data.

Render BusinessAnalysis.Rmd to generate dashboards.

📈 Example Outputs

Consolidated film & music revenue by channel and country

Time-series revenue trends (film vs. music)

Top customers and markets

👥 Author

Shaobo Chen
