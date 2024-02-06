# Airflow Demo

## Requirements

- Docker Desktop
- Docker Compose

## How to run

### 1. Clone Repo

```shell
$ git clone https://github.com/NimzyMaina/airflow_demo & cd airflow_demo
```

### 2. Initialize Airflow

```shell
$ docker-compose up airflow-init
```

### 3. Run the Airflow Server

```shell
$ docker-compose up
```

### 4. Setup Connections

**ClickHouse**

| Field    | Value |
| -------- | ------- |
| Connection ID  | click_house_01    |
| Connection Type  | sqlite    |
| Host | github.demo.trial.altinity.cloud    |
| Schema  | default    |
| Login  | demo    |
| Password  | demo    |
| Port  | 9440    |


**Sqlite**

| Field    | Value |
| -------- | ------- |
| Connection ID  | click_house_01    |
| Connection Type  | sqlite    |
| Host |  	/opt/airflow/dags/sql.db   |

### 5. Run DAG

Open Airflow Dashboard on your Browser -  [Airflow Dashboard](http://localhost:8080/home)

