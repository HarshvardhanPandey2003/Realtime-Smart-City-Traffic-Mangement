from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from kafka import KafkaProducer, KafkaConsumer
import json
import psycopg2

# Kafka Configuration
KAFKA_BROKER_URL = 'broker:9092'
VOTER_TOPIC = 'voters'
CANDIDATE_TOPIC = 'candidates'

# PostgreSQL Configuration
POSTGRES_CONN_PARAMS = {
    'host': 'postgres',
    'database': 'voting',
    'user': 'postgres',
    'password': 'postgres'
}

def generate_data():
    # Sample function to generate random voter and candidate data and send to Kafka
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    voter_data = {'voter_id': 'v123', 'candidate_id': 'c456'}  # Replace with actual data generation
    producer.send(VOTER_TOPIC, voter_data)
    
    candidate_data = {'candidate_id': 'c456', 'candidate_name': 'Candidate X'}
    producer.send(CANDIDATE_TOPIC, candidate_data)
    producer.flush()

def consume_and_process_data():
    # Consume data from Kafka and process it (e.g., mapping voter to candidate)
    consumer = KafkaConsumer(VOTER_TOPIC, CANDIDATE_TOPIC,
                             bootstrap_servers=KAFKA_BROKER_URL,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    
    for message in consumer:
        data = message.value
        # Process data (e.g., store vote for candidate mapping)
        if message.topic == VOTER_TOPIC:
            process_vote(data)  # Store voter to candidate relationship

def process_vote(vote_data):
    # Connect to PostgreSQL and store the vote data
    conn = psycopg2.connect(**POSTGRES_CONN_PARAMS)
    cur = conn.cursor()
    cur.execute("INSERT INTO votes (voter_id, candidate_id) VALUES (%s, %s)",
                (vote_data['voter_id'], vote_data['candidate_id']))
    conn.commit()
    cur.close()
    conn.close()

def aggregate_results():
    # Perform aggregation on the votes table to get results
    conn = psycopg2.connect(**POSTGRES_CONN_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT candidate_id, COUNT(*) AS vote_count FROM votes GROUP BY candidate_id")
    results = cur.fetchall()
    # For simplicity, print results, but you can save this back to a results table or for visualization
    print(results)
    conn.close()

# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'voting_system_dag',
    default_args=default_args,
    description='Voting System Workflow',
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    task_generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data
    )

    task_consume_and_process_data = PythonOperator(
        task_id='consume_and_process_data',
        python_callable=consume_and_process_data
    )

    task_aggregate_results = PythonOperator(
        task_id='aggregate_results',
        python_callable=aggregate_results
    )

    # DAG dependencies
    task_generate_data >> task_consume_and_process_data >> task_aggregate_results
