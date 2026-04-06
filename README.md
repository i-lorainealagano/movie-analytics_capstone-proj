🎬 Movie Analytics Pipeline (End-to-End Data Engineering Project)
Overview

This project builds a complete end-to-end data pipeline for movie analytics using modern data engineering tools.

It ingests raw movie datasets, processes them through ETL pipelines, transforms them using dbt, and delivers analytics-ready data for visualization.

Key Insights Generated
Movie rating trends over time
Language performance rankings
Genre popularity analysis
Country-level production insights

Architecture
Raw Data → Airflow ETL → PostgreSQL → dbt Models → Analytics Layer → Power BI Dashboard

Pipeline Flow
            ┌──────────────┐
            │   Raw Data   │
            └──────┬───────┘
                   │
                   ▼
        ┌────────────────────┐
        │  Extract & Clean   │
        │ (Python Scripts)   │
        └────────┬───────────┘
                 │
                 ▼
        ┌────────────────────┐
        │   Load to DB       │
        │   (PostgreSQL)     │
        └────────┬───────────┘
                 │
                 ▼
        ┌────────────────────┐
        │   dbt Transform    │
        │ (Staging → Marts)  │
        └────────┬───────────┘
                 │
                 ▼
        ┌────────────────────┐
        │   Data Testing     │
        │    (dbt test)      │
        └────────┬───────────┘
                 │
                 ▼
        ┌────────────────────┐
        │   Analytics Layer  │
        │   (Power BI)       │
        └────────────────────┘


Tech Stack
Orchestration: Apache Airflow
ETL: Python
Data Warehouse: PostgreSQL
Transformations: dbt
Containerization: Docker
Visualization: Power BI


Project Structure
movie-analytics-capstone-proj/
│
├── airflow/
│   ├── dags/
│   ├── scripts/
│   └── Dockerfile
│
├── data/
│
├── movies_analytics/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   ├── marts/
│   │   │   ├── dimensions/
│   │   │   ├── facts/
│   │   │   └── bridges/
│   ├── macros/
│   ├── seeds/
│   ├── snapshots/
│   └── tests/
│
├── notebooks/
├── scripts/
├── docker-compose.yaml
├── requirements.txt
└── README.md


Airflow ETL Pipeline
DAG: movies_analytics_etl

The pipeline is orchestrated using Airflow and runs daily:

Start → Extract → Load → dbt Run → dbt Test → End


Design Decisions

Why Airflow?
Handles scheduling and dependencies
Built-in retry and monitoring
Industry-standard orchestration tool

Why dbt?
Modular SQL transformations
Version-controlled models
Built-in testing and documentation

Why Docker?
Ensures consistent environments
Simplifies deployment
Isolates services (Airflow, dbt, Postgres)

Architecture Pattern
ELT (Extract → Load → Transform)

Layered modeling:
Staging
Intermediate
Marts (Star Schema)

Data Models

Fact Table
fact_movie_metrics

Dimensions
dim_movies
dim_languages
dim_genres
dim_countries
dim_production_companies


Bridge Tables
Movies ↔ Genres
Movies ↔ Languages
Movies ↔ Countries
Movies ↔ Companies


Getting Started
1. Clone repo
git clone <repo-url>
cd movie-analytics-capstone-proj
2. Run with Docker
docker-compose up --build
3. Run dbt
cd movies_analytics
dbt run
dbt test
Dashboard

Power BI dashboard file:

movie-analytics-system-dashboard.pbix
Future Improvements
Add real-time streaming (Kafka)
Deploy to cloud (AWS/GCP)
Add ML recommendation system
Implement incremental dbt models


Author
Loraine Angela Alagano