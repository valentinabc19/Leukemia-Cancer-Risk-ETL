import json
import pandas as pd
import os
from sqlalchemy import create_engine
from typing import Dict

ROOT_DIR = os.path.abspath(os.path.join(__file__, "../../../"))
CREDENTIALS_PATH = os.path.join(ROOT_DIR, "credentialsdb.json")  
    
def load_db_credentials(json_path: str) -> Dict[str, str]:
 
    """
    Loads database credentials from JSON file
    
    Args:
        json_path: Path to JSON credentials file
        
    Returns:
        Dictionary with connection parameters
        
    Raises:
        ValueError: If JSON file is invalid or missing required keys
    """
    try:
        with open(json_path, 'r') as f:
            creds = json.load(f)
            
        # Verify required keys are present
        required_keys = ['db_host', 'db_name', 'db_user', 'db_password']
        if not all(key in creds for key in required_keys):
            missing = set(required_keys) - set(creds.keys())
            raise ValueError(f"Missing required keys in credentials: {missing}")
            
        return creds
        
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON format in credentials file")
    except FileNotFoundError:
        raise ValueError(f"Credentials file not found at {json_path}")
    

def export_to_postgres(df_dict: Dict[str, pd.DataFrame], validation_success: bool ) -> None:
    """
    Exports all DataFrames to PostgreSQL database
    
    Args:
        df_dict: Dictionary of {table_name: DataFrame} to export
        
    Raises:
        RuntimeError: If export fails
    """

    if not validation_success:
        print("Validations failed. Unable to load data to PostgreSQL DB")
        return
    
    creds = load_db_credentials(CREDENTIALS_PATH)
    try:

        engine = create_engine(
            f"postgresql://{creds['db_user']}:{creds['db_password']}@"
            f"{creds['db_host']}:5432/{creds['db_name']}",
            connect_args={'connect_timeout': 10}
        )
        

        print("Successfully connected to PostgreSQL database")
        

        for table_name, df in df_dict.items():
            try:
                df.to_sql(
                    name=table_name.lower(),
                    con=engine,
                    if_exists='replace',
                    index=False,
                    chunksize=1000,
                    method='multi'
                )
                print(f"Successfully exported {table_name} ({len(df)} records)")
            except Exception as e:
                print(f"Failed to export {table_name}: {str(e)}")
                continue

    except Exception as e:
        raise RuntimeError(f"Database export failed: {str(e)}")
    finally:
        if 'engine' in locals():
            engine.dispose()
            print("Database connection closed")
    