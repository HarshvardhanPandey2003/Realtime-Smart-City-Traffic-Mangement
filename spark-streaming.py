import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
from psycopg2.extras import RealDictCursor
import json


# Set Python executable path
os.environ["PYSPARK_PYTHON"] = "python"  # or "python3" if that is what you named your executable
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"  # or "python3"

# Now initialize Spark
from pyspark.sql import SparkSession
# Define schema for the votes data
votes_schema = StructType([
    StructField("voter_id", StringType(), True),
    StructField("candidate_id", StringType(), True),
    StructField("voting_time", TimestampType(), True),
    StructField("vote", IntegerType(), True)
])

def get_postgres_data():
    """Fetch voter and candidate data from PostgreSQL"""
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    
    # Get candidates data
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT candidate_id, candidate_name, party_affiliation 
        FROM candidates
    """)
    candidates = cur.fetchall()
    
    # Get voters data
    cur.execute("""
        SELECT voter_id, voter_name, gender, address_state 
        FROM voters
    """)
    voters = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return candidates, voters

def create_temp_views(spark, candidates, voters):
    """Create temporary views for PostgreSQL data"""
    # Convert candidates to DataFrame
    candidates_df = spark.createDataFrame(candidates)
    candidates_df.createOrReplaceTempView("candidates")
    
    # Convert voters to DataFrame
    voters_df = spark.createDataFrame(voters)
    voters_df.createOrReplaceTempView("voters")

def main():
    # Create checkpoint directory
    checkpoint_dir = os.path.join(os.getcwd(), "checkpoints")
    os.makedirs(checkpoint_dir, exist_ok=True)

    # Initialize Spark Session with specific configurations for Windows
    spark = SparkSession.builder \
        .appName("VoteAnalysis") \
        .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Get PostgreSQL data
        candidates, voters = get_postgres_data()
        create_temp_views(spark, candidates, voters)
        
        # Read from Kafka
        votes_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "votes_topic") \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Parse JSON data
        parsed_df = votes_df.select(
            from_json(col("value").cast("string"), votes_schema).alias("data")
        ).select("data.*")
        
        # Register the streaming DataFrame as a temporary view
        parsed_df.createOrReplaceTempView("votes_stream")
        
        # Create aggregations
        party_wise_votes = spark.sql("""
            SELECT 
                c.party_affiliation,
                count(*) as vote_count,
                count(*) * 100.0 / sum(count(*)) over() as vote_percentage
            FROM votes_stream v
            JOIN candidates c ON v.candidate_id = c.candidate_id
            GROUP BY c.party_affiliation
        """)
        
        # Write to console for debugging
        console_query = party_wise_votes.writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()
        
        # Write to Kafka
        kafka_query = party_wise_votes.selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "party_wise_results") \
            .option("checkpointLocation", os.path.join(checkpoint_dir, "kafka")) \
            .outputMode("complete") \
            .start()
        
        # Wait for termination
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()