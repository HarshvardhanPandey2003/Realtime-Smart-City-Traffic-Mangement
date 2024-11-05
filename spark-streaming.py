import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime

# Define the schema for vote data coming from Kafka
votes_schema = StructType([
    StructField("voter_id", StringType(), True),
    StructField("candidate_id", StringType(), True),
    StructField("voting_time", TimestampType(), True),
    StructField("vote", IntegerType(), True)
])

def get_postgres_data():
    """
    Fetch data from the PostgreSQL database.
    Returns:
        candidates (list of dict): List of candidate data
        voters (list of dict): List of voter data
    """
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Fetch candidate data including party affiliation
    cur.execute("""
        SELECT candidate_id, candidate_name, party_affiliation, 
               biography, campaign_platform, photo_url
        FROM candidates
    """)
    candidates = cur.fetchall()
    
    # Fetch detailed voter data
    cur.execute("""
        SELECT voter_id, voter_name, gender, address_state,
               date_of_birth, nationality, registration_number,
               address_street, address_city, address_country,
               address_postcode, email, registered_age
        FROM voters
    """)
    voters = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return candidates, voters

def create_temp_views(spark, candidates, voters):
    """Create temporary views in Spark for reference data"""
    candidates_df = spark.createDataFrame(candidates)
    candidates_df.createOrReplaceTempView("candidates")
    
    voters_df = spark.createDataFrame(voters)
    voters_df.createOrReplaceTempView("voters")

def main():
    # Create checkpoint directory
    checkpoint_dir = os.path.join(os.getcwd(), "checkpoints")
    os.makedirs(checkpoint_dir, exist_ok=True)

    # Initialize SparkSession with required configurations
    spark = SparkSession.builder \
        .appName("VotingAnalysis") \
        .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .config("spark.driver.extraJavaOptions", "--add-exports java.base/sun.nio.ch=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions", "--add-exports java.base/sun.nio.ch=ALL-UNNAMED") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Load reference data
        candidates, voters = get_postgres_data()
        create_temp_views(spark, candidates, voters)
        
        # Read vote stream from Kafka
        votes_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "votes_topic") \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Parse JSON messages
        parsed_df = votes_df.select(
            from_json(col("value").cast("string"), votes_schema).alias("data")
        ).select("data.*")
        
        # Register streaming view
        parsed_df.createOrReplaceTempView("votes_stream")
        
        # Calculate party-wise statistics
        party_stats = spark.sql("""
            WITH vote_counts AS (
                SELECT 
                    c.party_affiliation,
                    COUNT(*) as vote_count,
                    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as vote_percentage
                FROM votes_stream v
                JOIN candidates c ON v.candidate_id = c.candidate_id
                GROUP BY c.party_affiliation
            )
            SELECT 
                party_affiliation,
                vote_count,
                ROUND(vote_percentage, 2) as vote_percentage,
                RANK() OVER (ORDER BY vote_count DESC) as rank
            FROM vote_counts
            ORDER BY vote_count DESC
        """)
        
        # Calculate state-wise statistics
        state_stats = spark.sql("""
            SELECT 
                vr.address_state,
                c.party_affiliation,
                COUNT(*) as vote_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY vr.address_state), 2) as state_percentage
            FROM votes_stream v
            JOIN voters vr ON v.voter_id = vr.voter_id
            JOIN candidates c ON v.candidate_id = c.candidate_id
            GROUP BY vr.address_state, c.party_affiliation
            ORDER BY vr.address_state, vote_count DESC
        """)
        
        # Write party-wise results to console
        party_query = party_stats.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .start()
        
        # Write state-wise results to console
        state_query = state_stats.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .start()
        
        # Write party-wise results to Kafka
        kafka_party_query = party_stats.selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "party_wise_results") \
            .option("checkpointLocation", os.path.join(checkpoint_dir, "kafka_party")) \
            .outputMode("complete") \
            .start()
        
        # Write state-wise results to Kafka
        kafka_state_query = state_stats.selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "state_wise_results") \
            .option("checkpointLocation", os.path.join(checkpoint_dir, "kafka_state")) \
            .outputMode("complete") \
            .start()
        
        # Keep the application running
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()