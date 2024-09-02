# Airbnb Data ETL Pipeline with Apache Airflow, Kaggle API, and Docker

## Project Overview

This project demonstrates a complete ETL (Extract, Transform, Load) pipeline for Airbnb data using **Apache Airflow**, **Kaggle API**, **Pandas**, **PostgreSQL**, and **Docker**. The primary goal of the project is to extract data from Kaggle, transform it using Python and Pandas, and then load it into a PostgreSQL database for further analysis. This ETL pipeline is designed to run in a Dockerized environment, ensuring a consistent and reproducible setup.

## Key Technologies

- **Apache Airflow**: Orchestrates the ETL workflow, schedules tasks, and manages dependencies.
- **Kaggle API**: Extracts the Airbnb dataset directly from Kaggle.
- **Pandas**: Transforms the raw data into a clean and structured format.
- **PostgreSQL**: Serves as the database to store the transformed data.
- **Docker**: Containerizes the entire ETL pipeline to ensure portability and ease of deployment.

## Project Architecture

The project consists of the following key steps:

1. **Extract**: Using the Kaggle API, the latest Airbnb dataset is downloaded.
2. **Transform**: The dataset is processed and cleaned using Pandas. This step includes handling missing values, normalizing data formats, and generating new features.
3. **Load**: The cleaned and transformed data is loaded into a PostgreSQL database.
4. **Docker**: All components, including Airflow, PostgreSQL, and the ETL script, are containerized using Docker for seamless deployment and management.


## Setup Instructions

Follow these steps to set up and run the ETL pipeline on your local machine:

### 1. Prerequisites

- Docker and Docker Compose installed
- Kaggle account and API token

### 2. Clone the Repository

```bash
git clone https://github.com/alrSasani/Kaggle_airbnb_ETL.git
cd Kaggle_airbnb_ETL
```

### 3 Build and Start the Docker Containers
docker-compose up --build

### 5. Access Apache Airflow
Once the containers are up and running, you can access the Apache Airflow web UI at http://localhost:8080. Use the default credentials:

Username: airflow

Password: airflow

### 6. Trigger the ETL DAG
Navigate to the Airflow web UI, locate the DAG named Kaggle_airbnb_ETL, and trigger it manually. Airflow will execute the ETL steps in the correct order, from extracting data to loading it into PostgreSQL.

### 7. Verify the Data in PostgreSQL
You can access the PostgreSQL database through a PostgreSQL client (like pgAdmin or psql) to verify the loaded data:

docker exec -it <postgres_container_name> psql -U airflow -d airbnb_db


## Project Highlights
End-to-End ETL Pipeline: Demonstrates a complete data pipeline from data extraction to data loading using industry-standard tools.

Containerization with Docker: Ensures a consistent environment across different setups, enhancing portability and reproducibility.

Data Transformation with Pandas: Showcases data manipulation and transformation skills using Python, a critical skill in data engineering roles.

Orchestration with Apache Airflow: Provides experience with workflow automation and orchestration using Apache Airflow, a popular tool in data engineering.

## Conclusion
This project serves as a practical demonstration of implementing a robust ETL pipeline using modern data engineering tools and practices. The use of Docker for containerization, Apache Airflow for orchestration, and PostgreSQL for data storage provides a comprehensive understanding of the ETL process in a scalable and reproducible manner.

Feel free to explore the repository, run the ETL pipeline, and use the setup for your data engineering tasks.

## Contact

For any questions or inquiries, please contact:

- **Name**: Alireza Sasani
- **Email**: alr.sasani@gmail.com

---