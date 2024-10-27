import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from confluent_kafka import Consumer
import json
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from collections import defaultdict
import time

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'streamlit_consumer_group',
    'auto.offset.reset': 'earliest'
}

# Database Configuration
DB_CONFIG = "host=localhost dbname=voting user=postgres password=postgres"

def get_db_connection():
    return psycopg2.connect(DB_CONFIG)

def load_reference_data():
    """Load candidate and voter data from PostgreSQL"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get candidates data
    cur.execute("""
        SELECT candidate_id, candidate_name, party_affiliation 
        FROM candidates
    """)
    candidates = {row['candidate_id']: row for row in cur.fetchall()}
    
    # Get voters data
    cur.execute("""
        SELECT voter_id, voter_name, gender, address_state 
        FROM voters
    """)
    voters = {row['voter_id']: row for row in cur.fetchall()}
    
    cur.close()
    conn.close()
    return candidates, voters

def process_votes():
    """Process votes from Kafka and maintain aggregations"""
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(['votes_topic'])
    
    # Initialize aggregation dictionaries
    party_votes = defaultdict(int)
    gender_party_votes = defaultdict(lambda: defaultdict(int))
    state_party_votes = defaultdict(lambda: defaultdict(int))
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
            
        try:
            vote_data = json.loads(msg.value())
            voter = voters.get(vote_data['voter_id'])
            candidate = candidates.get(vote_data['candidate_id'])
            
            if voter and candidate:
                party = candidate['party_affiliation']
                gender = voter['gender']
                state = voter['address_state']
                
                # Update aggregations
                party_votes[party] += 1
                gender_party_votes[gender][party] += 1
                state_party_votes[state][party] += 1
                
                # Convert to DataFrames for visualization
                party_df = pd.DataFrame([
                    {'party': k, 'votes': v} 
                    for k, v in party_votes.items()
                ])
                
                gender_df = pd.DataFrame([
                    {'gender': gender, 'party': party, 'votes': votes}
                    for gender, party_votes in gender_party_votes.items()
                    for party, votes in party_votes.items()
                ])
                
                state_df = pd.DataFrame([
                    {'state': state, 'party': party, 'votes': votes}
                    for state, party_votes in state_party_votes.items()
                    for party, votes in party_votes.items()
                ])
                
                yield party_df, gender_df, state_df
                
        except Exception as e:
            st.error(f"Error processing vote: {e}")
            continue

def create_dashboard():
    st.title("Real-time Voting Dashboard")
    
    # Initialize placeholder containers
    party_chart = st.empty()
    gender_chart = st.empty()
    state_chart = st.empty()
    
    # Load reference data
    global candidates, voters
    candidates, voters = load_reference_data()
    
    # Create three columns for metrics
    col1, col2, col3 = st.columns(3)
    total_votes = col1.empty()
    latest_update = col2.empty()
    active_states = col3.empty()
    
    # Process votes and update dashboard
    for party_df, gender_df, state_df in process_votes():
        # Update total votes metric
        total_votes.metric("Total Votes Cast", party_df['votes'].sum())
        
        # Update timestamp
        latest_update.metric("Last Updated", datetime.now().strftime("%H:%M:%S"))
        
        # Update active states metric
        active_states.metric("Active States", len(state_df['state'].unique()))
        
        # Party-wise vote distribution
        fig1 = px.pie(party_df, values='votes', names='party', 
                     title='Party-wise Vote Distribution')
        party_chart.plotly_chart(fig1, use_container_width=True)
        
        # Gender-wise voting patterns
        fig2 = px.bar(gender_df, x='party', y='votes', color='gender', 
                     title='Gender-wise Voting Patterns',
                     barmode='group')
        gender_chart.plotly_chart(fig2, use_container_width=True)
        
        # State-wise voting patterns
        fig3 = px.bar(state_df, x='state', y='votes', color='party',
                     title='State-wise Voting Patterns',
                     barmode='stack')
        state_chart.plotly_chart(fig3, use_container_width=True)
        
        # Short sleep to prevent overwhelming the browser
        time.sleep(0.1)

if __name__ == "__main__":
    create_dashboard()