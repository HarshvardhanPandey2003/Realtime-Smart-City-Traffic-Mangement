import random
import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer

# Modified for Indian voters and parties
BASE_URL = 'https://randomuser.me/api/?nat=in'
PARTIES = ["Bhartiya Janta Party", "Aam Aadmi Party", "Congress Party"]
random.seed(42)

def create_database():
    """Create the voting database if it doesn't exist"""
    try:
        # Connect to default postgres database first
        conn = psycopg2.connect(
            "host=localhost dbname=postgres user=postgres password=postgres"
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        # Check if database exists
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'voting'")
        exists = cur.fetchone()
        
        if not exists:
            cur.execute('CREATE DATABASE voting')
            print("Database 'voting' created successfully")
        else:
            print("Database 'voting' already exists")
            
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error creating database: {e}")
        raise e

def generate_voter_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": "Indian",  # Override nationality
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": "India",  # Override country
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
    else:
        return "Error fetching data"

def generate_candidate_data(candidate_number, total_parties):
    response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        
        # Indian-specific campaign platforms
        campaign_platforms = [
            "Focusing on digital India and economic growth.",
            "Fighting corruption and improving public services.",
            "Promoting inclusive development and social justice."
        ]
        
        return {
            "candidate_id": user_data['login']['uuid'],
            "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "party_affiliation": PARTIES[candidate_number % total_parties],
            "biography": f"A dedicated public servant from {user_data['location']['state']}, committed to India's progress.",
            "campaign_platform": campaign_platforms[candidate_number % total_parties],
            "photo_url": user_data['picture']['large']
        }
    else:
        return "Error fetching data"

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Kafka Topics
voters_topic = 'voters_topic'
candidates_topic = 'candidates_topic'

def create_tables(conn, cur):
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

def insert_voters(conn, cur, voter):
    cur.execute("""
        INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, 
        registration_number, address_street, address_city, address_state, address_country, 
        address_postcode, email, phone_number, cell_number, picture, registered_age)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
         voter['nationality'], voter['registration_number'], voter['address']['street'],
         voter['address']['city'], voter['address']['state'], voter['address']['country'],
         voter['address']['postcode'], voter['email'], voter['phone_number'],
         voter['cell_number'], voter['picture'], voter['registered_age'])
    )
    conn.commit()

if __name__ == "__main__":
    # Create database if it doesn't exist
    create_database()
    
    # Connect to the voting database
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})
    create_tables(conn, cur)

    # Get candidates from db
    cur.execute("SELECT * FROM candidates")
    candidates = cur.fetchall()
    print(candidates)

    if len(candidates) == 0:
        for i in range(3):
            candidate = generate_candidate_data(i, 3)
            print(candidate)
            cur.execute("""
                INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, 
                biography, campaign_platform, photo_url)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, 
                (candidate['candidate_id'], candidate['candidate_name'], 
                 candidate['party_affiliation'], candidate['biography'],
                 candidate['campaign_platform'], candidate['photo_url'])
            )
            conn.commit()

    for i in range(1000):
        voter_data = generate_voter_data()
        insert_voters(conn, cur, voter_data)

        producer.produce(
            voters_topic,
            key=voter_data["voter_id"],
            value=json.dumps(voter_data),
            on_delivery=delivery_report
        )

        print('Produced voter {}, data: {}'.format(i, voter_data))
        producer.flush()