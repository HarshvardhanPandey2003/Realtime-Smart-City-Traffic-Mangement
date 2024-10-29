import os
import json
import random
from datetime import datetime
import logging

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common.serialization import SimpleStringSchema, JsonDeserializationSchema
from pyflink.common.typeinfo import Types, BasicTypeInfo
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema,
    DeliveryGuarantee
)
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.common import Duration, Time as CommonTime

import psycopg2
from psycopg2.extras import execute_batch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_CONSUMER_GROUP_ID = 'flink_voting_consumer'
INPUT_TOPIC = 'voters_topic'
VOTES_TOPIC = 'votes_topic'
ANALYTICS_TOPIC = 'analytics_topic'
PARTY_ANALYTICS_TOPIC = 'party_analytics_topic'

# PostgreSQL Configuration
PG_CONFIG = {
    'host': 'localhost',
    'database': 'voting',
    'user': 'postgres',
    'password': 'postgres'
}

class VoteProcessor:
    def __init__(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.env.enable_checkpointing(interval=60000)  # 60 seconds
        self.env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
        
        # Set up JAR files
        self._add_jar_files()
        
        # Initialize candidates and party mapping
        self.candidates, self.candidate_party_map = self._get_candidates_data()
        logger.info(f"Loaded {len(self.candidates)} candidates")
        
    def _add_jar_files(self):
        """Add required JAR files to the Flink environment"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        jar_path = os.path.join(current_dir, 'lib')
        
        required_jars = [
            'flink-sql-connector-kafka-1.18.1.jar',
            'flink-connector-jdbc-1.18.1.jar',
            'flink-connector-base-1.18.1.jar',
            'kafka-clients-3.2.3.jar',
            'postgresql-42.7.2.jar'
        ]
        
        for jar in required_jars:
            jar_file = os.path.join(jar_path, jar)
            if os.path.exists(jar_file):
                self.env.add_jars(f"file://{jar_file}")
                logger.info(f"Added JAR: {jar}")
            else:
                raise FileNotFoundError(f"Required JAR file not found: {jar_file}")

    def _get_candidates_data(self):
        """Fetch candidates and their party affiliations from PostgreSQL"""
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT candidate_id, party_affiliation 
                FROM candidates
            """)
            results = cur.fetchall()
            candidates = [row[0] for row in results]
            party_map = {row[0]: row[1] for row in results}
            return candidates, party_map
        finally:
            cur.close()
            conn.close()

    def _create_kafka_source(self):
        """Create Kafka source for consuming voter data"""
        return KafkaSource.builder() \
            .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
            .set_topics(INPUT_TOPIC) \
            .set_group_id(KAFKA_CONSUMER_GROUP_ID) \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

    def _create_kafka_sink(self, topic):
        """Create Kafka sink for producing processed data"""
        serialization_schema = KafkaRecordSerializationSchema.builder() \
            .set_topic(topic) \
            .set_value_serialization_schema(SimpleStringSchema()) \
            .build()
        
        return KafkaSink.builder() \
            .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
            .set_record_serializer(serialization_schema) \
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
            .build()

    def _process_vote(self, voter_data):
        """Process individual vote data"""
        try:
            voter = json.loads(voter_data)
            voter_id = voter['voter_id']
            
            # Randomly select a candidate
            selected_candidate = random.choice(self.candidates)
            selected_party = self.candidate_party_map[selected_candidate]
            
            # Create vote record
            vote = {
                'voter_id': voter_id,
                'candidate_id': selected_candidate,
                'party': selected_party,
                'voting_time': datetime.now().isoformat(),
                'vote': 1,
                'voter_age': voter.get('registered_age', None),
                'voter_state': voter.get('address', {}).get('state', None)
            }
            
            # Save to PostgreSQL
            self._save_vote_to_db(vote)
            
            return json.dumps(vote)
        except Exception as e:
            logger.error(f"Error processing vote: {e}")
            return None

    def _save_vote_to_db(self, vote):
        """Save vote to PostgreSQL"""
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO votes (voter_id, candidate_id, voting_time, vote)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (voter_id) DO NOTHING
            """, (vote['voter_id'], vote['candidate_id'], 
                 vote['voting_time'], vote['vote']))
            conn.commit()
            logger.info(f"Vote saved for voter {vote['voter_id']}")
        except Exception as e:
            logger.error(f"Database error: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def _process_candidate_analytics(self, votes_window):
        """Process candidate-wise analytics for a window of votes"""
        try:
            vote_counts = {}
            for vote in votes_window:
                vote_data = json.loads(vote)
                candidate_id = vote_data['candidate_id']
                vote_counts[candidate_id] = vote_counts.get(candidate_id, 0) + 1
            
            analytics = {
                'timestamp': datetime.now().isoformat(),
                'window_size_seconds': 60,
                'candidate_votes': vote_counts,
                'total_votes': sum(vote_counts.values())
            }
            return json.dumps(analytics)
        except Exception as e:
            logger.error(f"Error processing candidate analytics: {e}")
            return None

    def _process_party_analytics(self, votes_window):
        """Process party-wise analytics for a window of votes"""
        try:
            party_votes = {}
            state_party_votes = {}
            age_group_votes = {}
            
            for vote in votes_window:
                vote_data = json.loads(vote)
                party = vote_data['party']
                state = vote_data['voter_state']
                age = vote_data.get('voter_age', 0)
                
                # Party-wise counting
                party_votes[party] = party_votes.get(party, 0) + 1
                
                # State-wise party counting
                if state:
                    if state not in state_party_votes:
                        state_party_votes[state] = {}
                    state_party_votes[state][party] = state_party_votes[state].get(party, 0) + 1
                
                # Age group counting
                age_group = f"{(age // 10) * 10}-{(age // 10) * 10 + 9}"
                if age_group not in age_group_votes:
                    age_group_votes[age_group] = {}
                age_group_votes[age_group][party] = age_group_votes[age_group].get(party, 0) + 1
            
            analytics = {
                'timestamp': datetime.now().isoformat(),
                'window_size_seconds': 60,
                'party_votes': party_votes,
                'state_party_votes': state_party_votes,
                'age_group_votes': age_group_votes,
                'total_votes': sum(party_votes.values())
            }
            return json.dumps(analytics)
        except Exception as e:
            logger.error(f"Error processing party analytics: {e}")
            return None

    def run(self):
        """Run the Flink voting application"""
        try:
            # Create Kafka source
            kafka_source = self._create_kafka_source()
            
            # Create data stream from source
            voter_stream = self.env.from_source(
                source=kafka_source,
                watermark_strategy=WatermarkStrategy.no_watermarks(),
                source_name="Kafka Voter Source"
            )

            # Process votes
            vote_stream = voter_stream \
                .map(self._process_vote) \
                .filter(lambda x: x is not None)

            # Write processed votes to Kafka
            vote_stream.sink_to(self._create_kafka_sink(VOTES_TOPIC))

            # Process candidate analytics with windowing
            candidate_analytics_stream = vote_stream \
                .window_all(TumblingEventTimeWindows.of(Time.seconds(60))) \
                .apply(self._process_candidate_analytics) \
                .filter(lambda x: x is not None)

            # Write candidate analytics to Kafka
            candidate_analytics_stream.sink_to(self._create_kafka_sink(ANALYTICS_TOPIC))

            # Process party analytics with windowing
            party_analytics_stream = vote_stream \
                .window_all(TumblingEventTimeWindows.of(Time.seconds(60))) \
                .apply(self._process_party_analytics) \
                .filter(lambda x: x is not None)

            # Write party analytics to Kafka
            party_analytics_stream.sink_to(self._create_kafka_sink(PARTY_ANALYTICS_TOPIC))

            # Execute the Flink job
            logger.info("Starting Flink Vote Processing Application")
            self.env.execute("Vote Processing Application")
            
        except Exception as e:
            logger.error(f"Error in Flink job execution: {e}")
            raise

if __name__ == "__main__":
    processor = VoteProcessor()
    processor.run()