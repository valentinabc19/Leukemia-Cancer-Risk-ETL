import os
from sqlalchemy import create_engine
import pandas as pd
import json

ROOT_DIR = os.path.abspath(os.path.join(__file__, "../../../"))
CREDENTIALS_PATH = os.path.join(ROOT_DIR, "credentials.json")   

def extract_data()-> pd.DataFrame:

    """
    Loads leukemia patient data from a PostgreSQL database using credentials from a JSON file.

    Returns:
        pd.DataFrame: DataFrame containing the extracted data.
    """

    with open(CREDENTIALS_PATH, "r", encoding="utf-8") as file:
        credentials = json.load(file)

    db_host = credentials["db_host"]
    db_name = credentials["db_name"]
    db_user = credentials["db_user"]
    db_password = credentials["db_password"]
    
    engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}")
    query = "SELECT * FROM leukemia_clean_data"
    with engine.connect() as conn:
        df = pd.read_sql(sql=query, con=conn.connection)
    
    return df
