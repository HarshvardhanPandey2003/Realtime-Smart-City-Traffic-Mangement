import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from confluent_kafka import Consumer
import json
from datetime import datetime
import plotly.express as px
from collections import defaultdict
import time

# Kafka and Database Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'streamlit_consumer_group',
    'auto.offset.reset': 'earliest'
}
DB_CONFIG = "host=localhost dbname=voting user=postgres password=postgres"

def get_db_connection():
    return psycopg2.connect(DB_CONFIG)

@st.cache_resource
def load_reference_data():
    """Load candidate and voter data from PostgreSQL."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Load candidates data
    cur.execute("SELECT candidate_id, candidate_name, party_affiliation FROM candidates")
    candidates = {row['candidate_id']: row for row in cur.fetchall()}
    
    # Load voters data
    cur.execute("SELECT voter_id, voter_name, gender, address_state, registered_age FROM voters")
    voters = {row['voter_id']: row for row in cur.fetchall()}
    
    cur.close()
    conn.close()
    return candidates, voters

def process_votes():
    """Process votes from Kafka and maintain aggregations."""
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(['votes_topic'])
    
    party_votes = defaultdict(int)
    gender_party_votes = defaultdict(lambda: defaultdict(int))
    state_party_votes = defaultdict(lambda: defaultdict(int))
    age_group_votes = defaultdict(int)
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        
        try:
            vote_data = json.loads(msg.value())
            voter = voters.get(vote_data['voter_id'])
            candidate = candidates.get(vote_data['candidate_id'])
            
            if voter and candidate:
                party = candidate['party_affiliation']
                gender = voter['gender']
                state = voter['address_state']
                age = voter['registered_age']
                
                # Define age group
                if age < 18:
                    continue
                elif age <= 25:
                    age_group = "18-25"
                elif age <= 35:
                    age_group = "26-35"
                elif age <= 45:
                    age_group = "36-45"
                elif age <= 60:
                    age_group = "46-60"
                else:
                    age_group = "60+"
                
                # Update aggregations
                party_votes[party] += 1
                gender_party_votes[gender][party] += 1
                state_party_votes[state][party] += 1
                age_group_votes[age_group] += 1
                
                # Convert to DataFrames for visualization
                party_df = pd.DataFrame([{'party': k, 'votes': v} for k, v in party_votes.items()])
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
                age_group_df = pd.DataFrame([{'age_group': k, 'votes': v} for k, v in age_group_votes.items()])
                
                yield party_df, gender_df, state_df, age_group_df
        except Exception as e:
            st.error(f"Error processing vote: {e}")
            continue

def create_dashboard():
    st.title("Real-time Voting Dashboard")
    
    global candidates, voters
    candidates, voters = load_reference_data()
    
    # Metrics section
    with st.container():
        col1, col2, col3 = st.columns(3)
        total_votes = col1.empty()
        latest_update = col2.empty()
        active_states = col3.empty()
    
    # Separate views in tabs for better organization
    tab1, tab2, tab3, tab4 = st.tabs([
        "Party-wise Distribution", "Gender-wise Distribution", "State-wise Distribution", "Age-wise Distribution"
    ])
    
    party_chart = tab1.empty()
    gender_chart = tab2.empty()
    state_chart = tab3.empty()
    age_chart = tab4.empty()
    
    for party_df, gender_df, state_df, age_group_df in process_votes():
        # Update total votes metric
        total_votes.metric("Total Votes Cast", party_df['votes'].sum())
        
        # Update timestamp
        latest_update.metric("Last Updated", datetime.now().strftime("%H:%M:%S"))
        
        # Update active states metric
        active_states.metric("Active States", len(state_df['state'].unique()))
        
        # Party-wise vote distribution (Tab 1)
        fig1 = px.pie(party_df, values='votes', names='party', 
                      title='Party-wise Vote Distribution',
                      color_discrete_sequence=px.colors.sequential.RdBu)
        fig1.update_traces(textinfo='percent+label')
        party_chart.plotly_chart(fig1, use_container_width=True)
        
        # Gender-wise voting patterns (Tab 2)
        fig2 = px.bar(gender_df, x='party', y='votes', color='gender', 
                      title='Gender-wise Voting Patterns',
                      barmode='group', color_discrete_sequence=px.colors.qualitative.Set3)
        fig2.update_layout(xaxis_title='Party', yaxis_title='Votes')
        gender_chart.plotly_chart(fig2, use_container_width=True)
        
        # State-wise voting patterns (Tab 3)
        fig3 = px.bar(state_df, x='state', y='votes', color='party',
                      title='State-wise Voting Patterns',
                      barmode='stack', color_discrete_sequence=px.colors.qualitative.Dark2)
        fig3.update_layout(xaxis_title='State', yaxis_title='Votes')
        state_chart.plotly_chart(fig3, use_container_width=True)
        
        # Age-wise voting patterns (Tab 4)
        fig4 = px.bar(age_group_df, x='age_group', y='votes', 
                      title='Age-wise Voting Patterns',
                      color='age_group', color_discrete_sequence=px.colors.qualitative.Pastel1)
        fig4.update_layout(xaxis_title='Age Group', yaxis_title='Votes')
        age_chart.plotly_chart(fig4, use_container_width=True)
        
        time.sleep(0.1)

if __name__ == "__main__":
    create_dashboard()
