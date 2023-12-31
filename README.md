# Fishing for α on the Lakehouse
## Overview
Personal project aiming to go the full A ~ Z in terms of data lifecycle incorporating data ingestion, processing, analytics & dashboarding, ML/DL as well as general software development for presentation. 

Uses stock price data at high frequencies (minutes and secods) to simulate high-latency data streams, as well as large sets of unstructured data ingestion. 

General architecture is a hybrid lambda + lakehouse, with separate speed and batch layers. Also incorporates usage of AWS S3 and delta lake abstraction to simulate both the data lake and warehouse (hence data lakehouse).


![gif](https://github.com/iamwonseokchoi/alpha_lakehouse/blob/main/images/gif.gif?raw=true)


![Predictions](images/dl.png)
--
![News](images/news.png)
--
![Indicators](images/technicals.png)

## Technologies
- **Languages:** Python, Scala
- **Tools:** Kafka, Spark, Terraform, Docker, Cassandra, Jupyter, FastAPI, Streamlit, ML/DL
- **Services:** AWS S3

## Data Used
Utilized [Polygon.io](https://polygon.io) financial API, and was incremented as follows:
- 1-second interval price data (stream)
- 1-minute interval technical indicator data (stream)
- News Data (batch)
- Realtime Websocket market data (client + stream)

## Architecture
![Architecture Overview](images/architecture.png)

## To Reproduce 
### Env. Variables
Simply uses `.env` configurations for secrets management
```
TICKERS = ["AAPL", "AMZN", "GOOGL", "MSFT", "NVDA", "TSLA"]

CASSANDRA_USERNAME = "cassandra"
CASSANDRA_PASSWORD = "cassandra"
AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"
POLYGON_API_KEY = "<Free key available @Polygon.io but capped to 5 API calls/minute>"
AWS_ACCESS_KEY = "<Required for S3 Terraform>"
AWS_ACCESS_KEY_SECRET = "<Can be generated using AWS IAM>"
```
### Step 1: Deploy Terraform
Change the global unique s3 storage name in terraform files then:
```
~ $ cd s3_data_lake 
~ $ terraform init
~ $ terraform plan 
~ $ terraform apply
# To destroy s3 lake: terraform destroy
# Destroy resources as the configs leave the bucket open for simplicity
```
### Step 2: Build Custom Docker images and launch infras
Most base images are synched to ARM64 
```
~ $ docker build -t custom-airflow -f Dockerfile.airflow .
~ $ docker build -t custom-cassandra -f Dockerfile.cassandra .
~ $ docker-compose up -d
```
Default ports used, but check `docker-compose.yaml`

### Step 3: Application Layer
Backend
```
~ $ cd app/backend
~ $ uvicorn main:app --reload
```
Frontend
```
~ $ cd app/frontend
~ $ streamlit run Homepage.py
```

## Progression Screenshots

![Docker Setup](images/docker.png)

- Airflow, Spark, Cassandra components can all communicate with each other
- Set up Airflow SparkSubmitOperator with networked spark cluster
- Also set up Spark via volume mount to the Airflow container for simple jobs

![Spark Operators](images/sparkOperator.png)

- Batch ingested second-level technical datas and minute-level price data to Cassandra
    - Optimized using scala for heavier stream loads with some tuning
- Pyspark:

![Pyspark Streams](images/stream_pyspark.png)
- Scala:

![Scala Streams](images/stream_scala.png)

- Was able to achieve almost parity for ~20K rows per second for processing and ingesting

![Tuned Scala](images/tuned_stream.png)

- Cassandra write-speed throttling to handle a heavy load for a single container with limited resources

![Cassandra](images/cassandra.png)

- Ran Periodic batches to sync and replicate to S3 lake

![Batch Jobs](images/batch.png)

- Lake data versioning using Delta abstraction as a sort of warehouse
    - Partitioned by timestamps for higher analytic read perfomance
    - Will work on batching for repartitioning or coalescing & file optimization later down the line to handle small-files

![S3 Lake](images/s3.png)

- Data pipeline and data architecture layers all complete including news data ingestion (semi-structured)
    - Speed and batch layer data all ingested and running either on Cassandra (speed layer) or Delta Lake (batch and speed layers)
    - Warehouse format for OLAP queries enforced for cleaned datasets, and Lake curation also complete
    - Can now build and test models using Jupyer notebook environment

![Batch Jobs](images/batch_complete.png)


## Caveats
To get minute-level data and connect to Polygon.io websocket, you require a paid API key.
Will maybe later upload a small sample for localizing.


**E.O.D**
