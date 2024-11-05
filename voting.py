import json
import random
from datetime import datetime
import psycopg2
from confluent_kafka import Consumer, Producer, KafkaError
import time

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
CONSUMER_GROUP_ID = 'voting_consumer_group'

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def get_candidates(cur):
    cur.execute("SELECT candidate_id FROM candidates")
    return [row[0] for row in cur.fetchall()]

def main():
    # Initialize Kafka consumer
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP_ID,
        'auto.offset.reset': 'earliest'
    })
    
    # Initialize Kafka producer
    producer = Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
    })
    
    # Connect to PostgreSQL
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()
    
    # Get list of candidates
    candidates = get_candidates(cur)
    if not candidates:
        print("No candidates found in database!")
        return
    
    # Subscribe to voters topic to get all the voters data 
    consumer.subscribe(['voters_topic'])
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('Reached end of partition')
                else:
                    print(f'Error: {msg.error()}')
                continue
            
            try:
                # Parse voter data
                voter_data = json.loads(msg.value())
                voter_id = voter_data['voter_id']
                
                # Simulate voting (randomly select a candidate)
                selected_candidate = random.choice(candidates)
                
                # Create vote data
                vote_data = {
                    'voter_id': voter_id,
                    'candidate_id': selected_candidate,
                    'voting_time': datetime.now().isoformat(),
                    'vote': 1
                }
                
                # Insert vote into PostgreSQL
                try:
                    cur.execute("""
                        INSERT INTO votes (voter_id, candidate_id, voting_time, vote)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (voter_id) DO NOTHING
                    """, (vote_data['voter_id'], vote_data['candidate_id'], 
                          vote_data['voting_time'], vote_data['vote']))
                    conn.commit()
                    
                    # The Poducer Produces the vote to Kafka votes_topic
                    producer.produce(
                        'votes_topic',
                        key=voter_id,
                        value=json.dumps(vote_data),
                        on_delivery=delivery_report
                    )
                    producer.flush()
                    
                    print(f"Processed vote for voter {voter_id} -> candidate {selected_candidate}")
                    
                except Exception as e:
                    print(f"Error processing vote: {e}")
                    conn.rollback()
                
            except json.JSONDecodeError as e:
                print(f"Error decoding message: {e}")
                continue
            
    except KeyboardInterrupt:
        print("Shutting down...")
    
    finally:
        # Clean up
        consumer.close()
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()