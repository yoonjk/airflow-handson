
## setup airflow

mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

## Initialize
docker-compose up airflow-init
