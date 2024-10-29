import os
import requests
from tqdm import tqdm

# Define the JAR files to download
jars = {
    "flink-sql-connector-kafka-1.18.1.jar": "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.18.1/flink-sql-connector-kafka-1.18.1.jar",
    "flink-connector-jdbc-1.18.1.jar": "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/1.18.1/flink-connector-jdbc-1.18.1.jar",
    "flink-connector-base-1.18.1.jar": "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-base/1.18.1/flink-connector-base-1.18.1.jar",
    "kafka-clients-3.2.3.jar": "https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar",
    "postgresql-42.7.2.jar": "https://jdbc.postgresql.org/download/postgresql-42.7.2.jar"
}

def download_file(url, filename):
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    
    with open(filename, 'wb') as file, tqdm(
        desc=filename,
        total=total_size,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as progress_bar:
        for data in response.iter_content(chunk_size=1024):
            size = file.write(data)
            progress_bar.update(size)

def main():
    # Create lib directory if it doesn't exist
    if not os.path.exists('lib'):
        os.makedirs('lib')
    
    # Download each JAR file
    for jar_name, url in jars.items():
        jar_path = os.path.join('lib', jar_name)
        if not os.path.exists(jar_path):
            print(f"Downloading {jar_name}...")
            download_file(url, jar_path)
            print(f"Downloaded {jar_name}")
        else:
            print(f"File {jar_name} already exists, skipping...")

if __name__ == "__main__":
    main()