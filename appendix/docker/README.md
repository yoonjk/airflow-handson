
## setup airflow in docker-compose

mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

## Initialize in docker-compose
docker-compose up airflow-init

## setup airflow on vm server
airflow db init

airflow users create \
 --username admin \
 --firstname Admin \
 --lastname spidyweb \
 --role Admin \
 --email admin@spidyweb.com

airflow webserver -p 8080 -D



