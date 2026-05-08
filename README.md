# 🚀 AWS Airflow DBT Data Pipeline

## 📌 Overview

This project implements a production-like **end-to-end data pipeline** that extracts data from PostgreSQL, orchestrates workflows with Apache Airflow, loads data incrementally into Snowflake, applies transformations and data quality validations with DBT, and delivers insights through a Looker Studio dashboard.

The environment is provisioned on **AWS EC2 (Ubuntu Linux)** using **Docker**, demonstrating both infrastructure setup and data engineering best practices.

---

## 🧱 Architecture

```id="arch-final"
PostgreSQL
    ↓
Airflow (Docker on EC2)
    ↓
Python ETL (Incremental Load)
    ↓
Snowflake (Data Warehouse)
    ↓
DBT (Transformations + Tests + Docs)
    ↓
Looker Studio (Dashboard)
```

---

## ⚙️ Tech Stack

* **Apache Airflow** – Workflow orchestration
* **Python** – ETL logic
* **PostgreSQL** – Source system
* **Snowflake** – Data warehouse
* **DBT (Data Build Tool)** – Transformations, testing, documentation
* **Docker** – Containerized environment
* **AWS EC2 (Ubuntu)** – Infrastructure
* **Looker Studio** – Data visualization

---

## ☁️ Infrastructure Setup (AWS EC2 + Docker)

The Airflow environment runs on an **Ubuntu EC2 instance** using Docker.

### Key Steps

* Provision EC2 (Ubuntu)
* Configure security group (**port 8080** for Airflow UI)
* Install Docker and Docker Compose
* Deploy Airflow via `docker-compose`
* Access UI via browser

```id="ec2-access"
http://<EC2-PUBLIC-IP>:8080
```

---

## 📂 Project Structure

```id="structure-final"
aws_airflow_dbt_docker_pipeline/
│
├── dags/                          # Airflow DAGs
│   └── postgres_to_snowflake.py
│
├── scripts/                       # Auxiliary scripts
├── dbt/                           # DBT models, tests, docs
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 🔄 Pipeline Flow

1. Extract data from PostgreSQL
2. Identify latest processed records in Snowflake
3. Perform **incremental extraction**
4. Dynamically process multiple tables
5. Load data into Snowflake
6. Apply DBT transformations
7. Validate business rules
8. Serve data for analytics

---

## 🔑 Key Features

* Incremental data loading (optimized performance)
* Dynamic DAG processing (multi-table)
* PostgreSQL → Snowflake integration
* Dockerized Airflow deployment on AWS
* Data transformation and validation with DBT
* End-to-end pipeline with analytics layer

---

## ✅ Data Quality & Business Rules (DBT)

Automated data validation using **DBT tests** ensures business consistency:

* ✔️ Sales price must not exceed the suggested price
* ✔️ Maximum discount per sale is limited to 5%

These checks run as part of the transformation layer, improving reliability and trust in the data.

---

## 📊 DBT Documentation

Auto-generated documentation and lineage using DBT:

```bash id="dbt-docs"
dbt docs generate
dbt docs serve
```

---

## 📊 Data Visualization (Looker Studio)

The final consumption layer is implemented using **Google Looker Studio**, enabling business users to explore insights interactively.

### Dashboard Highlights:

* Total sales and quantity KPIs
* Geographic distribution (map visualization)
* Sales by state (bar chart)
* Interactive filters (State and Dealership)

This layer represents the **business-facing output** of the pipeline.

---

## 🧠 End-to-End Flow

```id="flow-final"
PostgreSQL → Airflow → Snowflake → DBT → Looker Studio
```

---

## 🧪 How to Run (Local)

### 1. Clone the repository

```bash id="run-final-1"
git clone https://github.com/igorconceicao/aws_airflow_dbt_docker_pipeline.git
cd aws_airflow_dbt_docker_pipeline
```

### 2. Start Airflow

```bash id="run-final-2"
docker-compose up -d
```

### 3. Access UI

```id="run-final-3"
http://localhost:8080
```

Default credentials:

```id="run-final-4"
username: airflow
password: airflow
```

---

## 📈 Future Improvements

* Monitoring and alerting (Slack / Email)
* Deployment with MWAA or ECS
* CI/CD with GitHub Actions
* Advanced data quality (Great Expectations)
* Expand DBT models for analytics

---

## 👨‍💻 Author

**Igor Conceição**
Data Engineer

---

## ⭐ Support

If you found this project useful, feel free to star the repository.
