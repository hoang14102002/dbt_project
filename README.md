## dbt_project

## Overview

This is a **personal project** using **dbt (data build tool)** to build data pipelines on **SQL Server**.  
The goal is to practice transforming raw data into clean, analytical-ready tables using dbt models and SQL.

## Technologies

- **dbt**: Data transformation, modeling, and testing.
- **SQL Server**: Main relational database.
- **SQL**: Writing queries for data transformations in dbt models.

## Goals

- Learn and practice dbt with SQL Server.
- Build maintainable, production-ready data pipelines.
- Implement dbt concepts like models, tests, snapshots, and macros.

## Project Structure
dbt_project/
├── models/ # Transformation models (tables/views)
├── tests/ # Data tests
├── macros/ # Reusable macros
├── snapshots/ # Data snapshots (if any)
├── dbt_project.yml
└── README.md

## How to use
1. Clone the repository:
```bash
git clone https://github.com/hoang14102002/dbt_project.git
cd dbt_project

2. Install dbt (if not installed):
pip install dbt-sqlserver

3. Configure your profiles.yml to connect to SQL Server:
your_profile_name:
  target: dev
  outputs:
    dev:
      type: sqlserver
      driver: 'ODBC Driver 17 for SQL Server'
      server: <server_name>
      database: <database_name>
      schema: <schema_name>
      user: <username>
      password: <password>

4. Run dbt commands:
dbt run            # Run models
dbt test           # Run tests
dbt docs generate  # Generate documentation
dbt docs serve     # View documentation
