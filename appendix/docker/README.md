
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

sudo yum install @postgresql:9.6
or
sudo yum install @postgresql

sudo postgresql-setup --initdb

sudo passwd postgres

sudo systemctl start postgresql ## <-- start the server ##
sudo systemctl stop postgresql ## <-- stop the server ##
sudo systemctl restart postgresql ## <-- resstart the server ##
sudo systemctl status postgresql ## <-- get status of the server ##

sudo systemctl enable postgresql

sudo -i -u postgres

# Setup postgresql for airflow
CREATE DATABASE airflow;
CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
