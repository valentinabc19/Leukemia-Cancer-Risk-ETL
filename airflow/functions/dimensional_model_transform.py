import pandas as pd
from typing import Dict, Any


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
    ]]
    
    medical_history = medical_history.drop_duplicates().fillna('Unknown')
    medical_history.reset_index(drop=True, inplace=True)
    medical_history['medical_history_id'] = medical_history.index + 1
    
    return medical_history[['medical_history_id'] + medical_history.columns[:-1].tolist()]


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
    region['region_id'] = region.index + 1
    return region[['region_id', 'country']]


def extract_leukemia_facts(df: pd.DataFrame, patient_ids: Dict[Any, int], region_ids: Dict[str, int]) -> pd.DataFrame:

    """Extracts facts using your actual column names"""
    facts = df[[
        'id', 'country', 'wbc_count', 'rbc_count', 'platelet_count',
        'hemoglobin_level', 'bone_marrow_blasts', 'bmi', 'leukemia_status', 'living_status', 
        'co2_emissions_per_capita', 'nuclear_energy_pct', 'agri_employment_pct',
        'fertilizer_consumption', 'gdp_per_capita',	'pm25_pollution', 'undernourishment_rate', 'alcohol_consumption_liters'
    ]].copy().rename(columns={
        'id': 'patient_id'
    })
    
    facts['patient_id'] = facts['patient_id'].map(patient_ids)
    facts['region_id'] = facts['country'].map(region_ids)
    return facts.drop(columns=['country'])


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
        'id': 'patient_id'
    })
    
    patient_info['medical_history_id'] = patient_info['medical_key'].map(medical_history_ids)
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
    
    medical_history_ids = dict(zip(medical_keys.unique(), dim_medical['medical_history_id']))
    patient_ids = dict(zip(df['id'], df['id'])) 
    region_ids = dict(zip(dim_region['country'], dim_region['region_id']))

    dim_patient = extract_patient_info(df, medical_history_ids)
    fact_leukemia = extract_leukemia_facts(df, patient_ids, region_ids)
    
    return {
        'Fact_Leukemia': fact_leukemia,
        'Dim_Region': dim_region,
        'Dim_PatientInfo': dim_patient,
        'Dim_MedicalHistory': dim_medical,
    }


'''def main(data_path: str, creds_path: str) -> None:

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
CREDS_FILE = "/home/ubuntu/Escritorio/Leukemia-Cancer-Risk-ETL/credentialsdb.json"'''
