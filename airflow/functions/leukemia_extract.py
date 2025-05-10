import os
from sqlalchemy import create_engine
import pandas as pd
import json

def extract_data()-> pd.DataFrame:

    """ Extract data of the leukemia patients from the database in PostgreSQL."""

    try:
        os.chdir("../../Leukemia-Cancer-Risk-ETL")
    except FileNotFoundError:
        print("""
            FileNotFoundError - The directory may not exist or you are not located in the specified path.
            """)
    os.chdir("..")
    print(os.getcwd())

    with open("Leukemia-Cancer-Risk-ETL/credentials.json", "r", encoding="utf-8") as file:
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
