# Transform the data
import os
import json
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from typing import Dict, Any


def extract_data():

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


def extract_medical_history(df: pd.DataFrame) -> pd.DataFrame:

    """
    Extracts and transforms data for MedicalHistory dimension
    
    Args:
        df: Source DataFrame containing raw data
        
    Returns:
        Processed DataFrame for Dim_MedicalHistory
    """
   
    medical_history = df[[
        'genetic_mutation', 'family_history', 'smoking_status',
        'alcohol_consumption', 'radiation_exposure', 'infection_history',
        'chronic_illness', 'immune_disorders'
    ]].copy().rename(columns={
        'genetic_mutation': 'Genetic_Mutation',
        'family_history': 'Family_History',
        'smoking_status': 'Smoking_Status',
        'alcohol_consumption': 'Alcohol_Consumption',
        'radiation_exposure': 'Radiation_Exposure',
        'infection_history': 'Infection_History',
        'chronic_illness': 'Chronic_Illness',
        'immune_disorders': 'Immune_Disorders'
    })
    
    medical_history = medical_history.drop_duplicates().fillna('Unknown')
    medical_history.reset_index(drop=True, inplace=True)
    medical_history['MedicalHistoryId'] = medical_history.index + 1
    
    return medical_history[['MedicalHistoryId'] + medical_history.columns[:-1].tolist()]


def extract_region(df: pd.DataFrame) -> pd.DataFrame:

    """
    Extracts and transforms data for Region dimension
    
    Args:
        df: Source DataFrame containing raw data
        
    Returns:
        Processed DataFrame for Dim_Region
    """
    region = df[['country']].copy().drop_duplicates()
    region.reset_index(drop=True, inplace=True)
    region['RegionId'] = region.index + 1
    return region[['RegionId', 'country']]


def extract_leukemia_facts(df: pd.DataFrame, patient_ids: Dict[Any, int], region_ids: Dict[str, int]) -> pd.DataFrame:

    """Extracts facts using your actual column names"""
    facts = df[[
        'id', 'country', 'wbc_count', 'rbc_count', 'platelet_count',
        'hemoglobin_level', 'bone_marrow_blasts', 'bmi', 'leukemia_status'
    ]].copy().rename(columns={
        'id': 'PatientId',
        'country': 'Country',
        'wbc_count': 'WBC_Count',
        'rbc_count': 'RBC_Count',
        'platelet_count': 'Platelet_Count',
        'hemoglobin_level': 'Hemoglobin_level',
        'bone_marrow_blasts': 'Bone_Marrow_Blasts',
        'bmi': 'BMI',
        'leukemia_status': 'Leukemia_Status'
    })
    
    facts['PatientId'] = facts['PatientId'].map(patient_ids)
    facts['RegionId'] = facts['Country'].map(region_ids)
    return facts.drop(columns=['Country'])


def extract_patient_info(df: pd.DataFrame, medical_history_ids: Dict[Any, int]) -> pd.DataFrame:
 
    """
    Extracts and transforms data for PatientInfo dimension
    
    Args:
        df: Source DataFrame containing raw data
        medical_history_ids: Mapping dictionary for medical history IDs
        
    Returns:
        Processed DataFrame for Dim_PatientInfo
    """
    df['medical_key'] = df[
        ['genetic_mutation', 'family_history', 'smoking_status',
         'alcohol_consumption', 'radiation_exposure', 'infection_history',
         'chronic_illness', 'immune_disorders']
    ].astype(str).apply(lambda x: '|'.join(x), axis=1)
    
    patient_info = df[[
        'id', 'age', 'gender', 'socioeconomic_status', 'urban_rural', 'medical_key'
    ]].copy().rename(columns={
        'id': 'Patient_id',
        'age': 'Age',
        'gender': 'Gender',
        'urban_rural': 'Urban_Rural',
        'socioeconomic_status': 'Socioeconomic_Status'
    })
    
    patient_info['MedicalHistoryId'] = patient_info['medical_key'].map(medical_history_ids)
    return patient_info.drop(columns=['medical_key'])


def process_dimensions(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:

    """
    Orchestrates the complete dimension processing pipeline
    
    Args:
        df: Source DataFrame containing raw data
        
    Returns:
        Dictionary of processed DataFrames for all dimensions
    """

    dim_medical = extract_medical_history(df)
    dim_region = extract_region(df)
    
    medical_keys = df[
        ['genetic_mutation', 'family_history', 'smoking_status',
         'alcohol_consumption', 'radiation_exposure', 'infection_history',
         'chronic_illness', 'immune_disorders']
    ].astype(str).apply(lambda x: '|'.join(x), axis=1)
    
    medical_history_ids = dict(zip(medical_keys.unique(), dim_medical['MedicalHistoryId']))
    patient_ids = dict(zip(df['id'], df['id'])) 
    region_ids = dict(zip(dim_region['country'], dim_region['RegionId']))

    dim_patient = extract_patient_info(df, medical_history_ids)
    fact_leukemia = extract_leukemia_facts(df, patient_ids, region_ids)
    
    return {
        'Dim_MedicalHistory': dim_medical,
        'Dim_Region': dim_region,
        'Dim_PatientInfo': dim_patient,
        'Fact_Leukemia': fact_leukemia
    }


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


def export_to_postgres(df_dict: Dict[str, pd.DataFrame], creds: Dict[str, str], schema: str = None) -> None:
    """
    Exports all DataFrames to PostgreSQL database
    
    Args:
        df_dict: Dictionary of {table_name: DataFrame} to export
        creds: Database credentials dictionary
        schema: Target schema name (optional)
        
    Raises:
        RuntimeError: If export fails
    """
    try:
  
        engine = create_engine(
            f"postgresql://{creds['db_user']}:{creds['db_password']}@"
            f"{creds['db_host']}:5432/{creds['db_name']}",
            connect_args={'connect_timeout': 10}
        )
        
   
        with engine.connect() as conn:
            print("Successfully connected to PostgreSQL database")
        

        for table_name, df in df_dict.items():
            try:
                df.to_sql(
                    name=table_name.lower(),
                    con=engine,
                    schema=schema,
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


def main(data_path: str, creds_path: str) -> None:

    """
    Main execution function for complete ETL pipeline
    
    Args:
        data_path: Path to source data file
        creds_path: Path to credentials JSON file
    """
    try:
        print(f"Loading data from {data_path}")
        df = pd.read_csv(data_path)
        
        print("Processing data dimensions...")
        dimensions = process_dimensions(df)
     
        print(f"Loading credentials from {creds_path}")
        creds = load_db_credentials(creds_path)
        
        print("Exporting to PostgreSQL database...")
        export_to_postgres(dimensions, creds)
        
        print("ETL pipeline completed successfully!")
        
    except Exception as e:
        print(f"ETL pipeline failed: {str(e)}")
        raise


DATA_FILE = "/home/ubuntu/Escritorio/Leukemia-Cancer-Risk-ETL/data/biased_leukemia_dataset.csv"
CREDS_FILE = "/home/ubuntu/Escritorio/Leukemia-Cancer-Risk-ETL/credentialsdb.json"
