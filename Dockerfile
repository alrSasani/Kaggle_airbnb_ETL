# Image
FROM apache/airflow:2.9.3

# Create working directory
WORKDIR /opt/airflow
COPY . ./

# Install dependencies
RUN pip install -r requirements.txt

USER root
RUN chown airflow:$(id -g airflow) /opt/airflow/config/.kaggle/kaggle.json

USER airflow
RUN chmod 600 /opt/airflow/config/.kaggle/kaggle.json

# Set the environment variable to point to the kaggle.json location
ENV KAGGLE_CONFIG_DIR=/opt/airflow/config/.kaggle/