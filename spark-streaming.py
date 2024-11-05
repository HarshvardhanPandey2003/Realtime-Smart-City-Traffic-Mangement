import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
from psycopg2.extras import RealDictCursor
import json

# Set Python executable path for PySpark (modify if necessary)
os.environ["PYSPARK_PYTHON"] = "python"  # or "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"  # or "python3"

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
    Retrieves candidate and voter data to enrich the stream data.
    Returns:
        candidates (list of dict): List of candidate data with `candidate_id`, `candidate_name`, and `party_affiliation`.
        voters (list of dict): List of voter data with `voter_id`, `voter_name`, `gender`, and `address_state`.
    """
    # Connect to PostgreSQL
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Fetch candidate data
    cur.execute("""
        SELECT candidate_id, candidate_name, party_affiliation 
        FROM candidates
    """)
    candidates = cur.fetchall()
    
    # Fetch voter data
    cur.execute("""
        SELECT voter_id, voter_name, gender, address_state 
        FROM voters
    """)
    voters = cur.fetchall()
    
    # Close the database connection
    cur.close()
    conn.close()
    
    return candidates, voters

def create_temp_views(spark, candidates, voters):
    """
    Create temporary views in Spark for PostgreSQL data.
    Args:
        spark (SparkSession): The Spark session.
        candidates (list of dict): Candidate data from PostgreSQL.
        voters (list of dict): Voter data from PostgreSQL.
    """
    # Convert candidate data to DataFrame and register as a temporary view
    candidates_df = spark.createDataFrame(candidates)
    candidates_df.createOrReplaceTempView("candidates")
    
    # Convert voter data to DataFrame and register as a temporary view
    voters_df = spark.createDataFrame(voters)
    voters_df.createOrReplaceTempView("voters")

def main():
    # Create a checkpoint directory for Spark streaming
    checkpoint_dir = os.path.join(os.getcwd(), "checkpoints")
    os.makedirs(checkpoint_dir, exist_ok=True)

    # Initialize SparkSession with configurations for streaming and Kafka integration
    spark = SparkSession.builder \
        .appName("VoteAnalysis") \
        .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set Spark log level for monitoring purposes
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Retrieve candidate and voter data from PostgreSQL and create Spark temporary views
        candidates, voters = get_postgres_data()
        create_temp_views(spark, candidates, voters)
        
        # Set up the Kafka source to read the vote data stream
        votes_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "votes_topic") \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Parse the incoming JSON messages according to the predefined schema
        parsed_df = votes_df.select(
            from_json(col("value").cast("string"), votes_schema).alias("data")
        ).select("data.*")
        
        # Register the parsed data as a temporary view to facilitate SQL operations
        parsed_df.createOrReplaceTempView("votes_stream")
        
        # Define SQL query to calculate party-wise vote counts and percentages
        party_wise_votes = spark.sql("""
            SELECT 
                c.party_affiliation,
                count(*) as vote_count,
                count(*) * 100.0 / sum(count(*)) over() as vote_percentage
            FROM votes_stream v
            JOIN candidates c ON v.candidate_id = c.candidate_id
            GROUP BY c.party_affiliation
        """)
        
        # Stream the results to the console for verification/debugging
        console_query = party_wise_votes.writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()
        
        # Stream the results to a Kafka topic named "party_wise_results"
        kafka_query = party_wise_votes.selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "party_wise_results") \
            .option("checkpointLocation", os.path.join(checkpoint_dir, "kafka")) \
            .outputMode("complete") \
            .start()
        
        # Keep the Spark streaming job running until termination
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        # Handle exceptions by logging the error and stopping Spark
        print(f"Error occurred: {str(e)}")
        spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()
    