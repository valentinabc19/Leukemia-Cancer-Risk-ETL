import json
import os
import numpy as np
import pandas as pd
from kafka import KafkaProducer
from sqlalchemy import create_engine

# Define path to credentials file
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDENTIALS_PATH = os.path.join(ROOT_DIR, "credentials.json")


def convert_types(obj):
    """
    Convert NumPy data types to native Python types for JSON serialization.

    Args:
        obj: The object to convert.

    Returns:
        The converted object, compatible with JSON serialization.
    """
    if isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        return float(obj)
    elif isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    return obj


def send_to_kafka(df: pd.DataFrame, topic: str = 'fact_table',
                  bootstrap_servers: str = None) -> None:
    """
    Sends values from the 'wbc_count' column of a DataFrame to a Kafka topic.

    Args:
        df (pd.DataFrame): DataFrame containing the data.
        topic (str): Kafka topic name.
        bootstrap_servers (str): Kafka bootstrap server address. If None, uses environment variable or defaults to localhost.
    """
    if df.empty:
        print("Warning: DataFrame is empty. No messages will be sent.")
        return

    if bootstrap_servers is None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=convert_types).encode('utf-8')
    )

    for _, row in df.iterrows():
        message = {"wbc_count": row["wbc_count"]}
        future = producer.send(topic, message)
        try:
            record_metadata = future.get(timeout=10)
            print(f"Sent: {message} → topic: {record_metadata.topic}, partition: {record_metadata.partition}")
        except Exception as e:
            print(f"Error sending message: {e}")

    producer.flush()
    producer.close()


def extract_fact_table() -> pd.DataFrame:
    """
    Extracts leukemia patient data from a PostgreSQL database using credentials from a JSON file.

    Returns:
        pd.DataFrame: A DataFrame containing the 'wbc_count' column.

    Raises:
        ValueError: If the 'wbc_count' column is missing from the result.
        FileNotFoundError: If the credentials file is not found.
        KeyError: If required credentials are missing from the file.
    """
    if not os.path.exists(CREDENTIALS_PATH):
        raise FileNotFoundError(f"Credentials file not found at {CREDENTIALS_PATH}")

    with open(CREDENTIALS_PATH, "r", encoding="utf-8") as file:
        credentials = json.load(file)

    try:
        db_host = credentials["db_host"]
        db_name = credentials["db_name"]
        db_user = credentials["db_user"]
        db_password = credentials["db_password"]
    except KeyError as e:
        raise KeyError(f"Missing database credential: {e}")

    engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}")
    query = "SELECT wbc_count FROM fact_leukemia"

    with engine.connect() as conn:
        df = pd.read_sql(sql=query, con=conn.connection)

    if "wbc_count" not in df.columns:
        raise ValueError("'wbc_count' column is not present in the DataFrame")

    return df


def run_kafka_producer() -> None:
    """
    Extracts data from the database and sends it to the specified Kafka topic.
    """
    df = extract_fact_table()
    send_to_kafka(df, topic='fact_table')

