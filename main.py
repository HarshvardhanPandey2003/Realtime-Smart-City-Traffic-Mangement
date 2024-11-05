import random
import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer
from typing import List, Dict
import math

BASE_URL = 'https://randomuser.me/api/'
PARTIES = ["Bhartiya Janta Party", "Congress Party", "Aam Aadmi Party"]
BATCH_SIZE = 500  # Maximum number of results per API call
random.seed(42)

def create_tables(conn, cur):
    """Create necessary database tables if they don't exist"""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)

    conn.commit()

def delivery_report(err, msg):
    """Callback function to handle Kafka message delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def generate_voter_data_batch(batch_size: int) -> List[Dict]:
    """Fetch multiple voter records in a single API call"""
    response = requests.get(f"{BASE_URL}?nat=in&results={batch_size}")
    if response.status_code == 200:
        users_data = response.json()['results']
        return [
            {
                "voter_id": user['login']['uuid'],
                "voter_name": f"{user['name']['first']} {user['name']['last']}",
                "date_of_birth": user['dob']['date'],
                "gender": user['gender'],
                "nationality": user['nat'],
                "registration_number": user['login']['username'],
                "address": {
                    "street": f"{user['location']['street']['number']} {user['location']['street']['name']}",
                    "city": user['location']['city'],
                    "state": user['location']['state'],
                    "country": user['location']['country'],
                    "postcode": user['location']['postcode']
                },
                "email": user['email'],
                "phone_number": user['phone'],
                "cell_number": user['cell'],
                "picture": user['picture']['large'],
                "registered_age": user['registered']['age']
            }
            for user in users_data
        ]
    return []

def generate_candidate_data_batch(num_candidates: int) -> List[Dict]:
    """Fetch multiple candidate records in a single API call"""
    response = requests.get(f"{BASE_URL}?nat=in&results={num_candidates}")
    if response.status_code == 200:
        users_data = response.json()['results']
        return [
            {
                "candidate_id": user['login']['uuid'],
                "candidate_name": f"{user['name']['first']} {user['name']['last']}",
                "party_affiliation": PARTIES[i % len(PARTIES)],
                "biography": "A brief bio of the candidate.",
                "campaign_platform": "Key campaign promises or platform.",
                "photo_url": user['picture']['large']
            }
            for i, user in enumerate(users_data)
        ]
    return []

def batch_insert_voters(conn, cur, voters: List[Dict]):
    """Insert multiple voters in a single database transaction"""
    values = [(
        voter["voter_id"], 
        voter['voter_name'],
        voter['date_of_birth'],
        voter['gender'],
        voter['nationality'],
        voter['registration_number'],
        voter['address']['street'],
        voter['address']['city'],
        voter['address']['state'],
        voter['address']['country'],
        voter['address']['postcode'],
        voter['email'],
        voter['phone_number'],
        voter['cell_number'],
        voter['picture'],
        voter['registered_age']
    ) for voter in voters]
    
    cur.executemany("""
        INSERT INTO voters (
            voter_id, voter_name, date_of_birth, gender, nationality, 
            registration_number, address_street, address_city, address_state, 
            address_country, address_postcode, email, phone_number, 
            cell_number, picture, registered_age
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, values)
    conn.commit()

if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})
    create_tables(conn, cur)

    # Handle candidates
    cur.execute("SELECT * FROM candidates")
    candidates = cur.fetchall()

    if len(candidates) == 0:
        candidate_batch = generate_candidate_data_batch(3)
        for candidate in candidate_batch:
            cur.execute("""
                INSERT INTO candidates (
                    candidate_id, candidate_name, party_affiliation, 
                    biography, campaign_platform, photo_url
                )
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                candidate['candidate_id'], 
                candidate['candidate_name'],
                candidate['party_affiliation'],
                candidate['biography'],
                candidate['campaign_platform'],
                candidate['photo_url']
            ))
        conn.commit()

    # Handle voters in batches
    total_voters = 1000
    num_batches = math.ceil(total_voters / BATCH_SIZE)
    
    for batch in range(num_batches):
        batch_size = min(BATCH_SIZE, total_voters - (batch * BATCH_SIZE))
        voter_batch = generate_voter_data_batch(batch_size)
        
        # Insert voters into database
        batch_insert_voters(conn, cur, voter_batch)
        
        # Produce Kafka messages
        for voter in voter_batch:
            producer.produce(
                'voters_topic',
                key=voter["voter_id"],
                value=json.dumps(voter),
                on_delivery=delivery_report
            )
        
        print(f'Produced batch {batch + 1}/{num_batches}, {len(voter_batch)} voters')
        producer.flush()

    conn.close()