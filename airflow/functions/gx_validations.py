import great_expectations as gx
from great_expectations.dataset import PandasDataset
import pandas as pd
from typing import Dict
from great_expectations.core import ExpectationSuiteValidationResult


def validate_dim_patient_info(df: pd.DataFrame) -> ExpectationSuiteValidationResult:
    dataset = PandasDataset(df)
    
    dataset.expect_column_values_to_not_be_null("Patient_id")
    dataset.expect_column_values_to_be_unique("Patient_id")
    dataset.expect_column_values_to_be_in_set("Gender", ["Male", "Female"])
    dataset.expect_column_values_to_be_between("Age", min_value=0, max_value=110)
    dataset.expect_column_values_to_be_in_set("Socioeconomic_Status", ["Low", "Medium", "High"])
    dataset.expect_column_values_to_be_in_set("Urban_Rural", ["Urban", "Rural"])
    
    result = dataset.validate()
    return result


def validate_dim_medical_history(df: pd.DataFrame) -> ExpectationSuiteValidationResult:
    dataset = PandasDataset(df)
    
    dataset.expect_column_values_to_not_be_null("MedicalHistoryId")
    dataset.expect_column_values_to_be_unique("MedicalHistoryId")

    binary_cols = [
        "Family_History", "Smoking_Status", "Alcohol_Consumption", "Radiation_Exposure",
        "Infection_History", "Chronic_Illness", "Inmune_Disorders", "Genetic_Mutation"
    ]
    
    for col in binary_cols:
        dataset.expect_column_values_to_be_in_set(col, [0, 1])
    
    result = dataset.validate()
    return result


def validate_dim_region(df: pd.DataFrame) -> ExpectationSuiteValidationResult:
    dataset = PandasDataset(df)

    dataset.expect_column_values_to_not_be_null("RegionId")
    dataset.expect_column_values_to_be_unique("RegionId")
    dataset.expect_column_values_to_not_be_null("country")
    dataset.expect_column_values_to_match_regex("country", r"^[A-Za-z\s]+$")

    result = dataset.validate()
    return result


def validate_fact_leukemia(df: pd.DataFrame) -> ExpectationSuiteValidationResult:
    dataset = PandasDataset(df)

    dataset.expect_column_values_to_not_be_null("PatientId")
    dataset.expect_column_values_to_not_be_null("RegionId")
    dataset.expect_column_values_to_be_in_set("Leukemia_Status", [0, 1])
    dataset.expect_column_values_to_be_in_set("Living_Status", [0, 1])

    numeric_cols = [
        "WBC_Countsort", "RBC_Countsort", "Platelet_Countsort", "Hemoglobin_level", 
        "Bone_Marrow_Blasts", "BMI", "Fertilizer_consumption", "Gdp_per_capita",
        "Pm25_pollution", "Undernourishment_grade", "Agri_employment_pct",
        "Co2_emissions_per_capita", "Nuclear_energy_pct", "Alcohol_consumption_liters"
    ]
    
    for col in numeric_cols:
        dataset.expect_column_values_to_be_of_type(col, "float")
        dataset.expect_column_values_to_not_be_null(col)
        dataset.expect_column_values_to_be_between(col, min_value=0)

    result = dataset.validate()
    return result


def validate_all(
    df_patient_info: pd.DataFrame,
    df_medical_history: pd.DataFrame,
    df_region: pd.DataFrame,
    df_facts: pd.DataFrame
) -> Dict[str, ExpectationSuiteValidationResult]:
    """
    Validate all dimensions and facts using Great Expectations.

    Returns:
        Dictionary with validation results.
    """
    return {
        "Dim_PatientInfo": validate_dim_patient_info(df_patient_info),
        "Dim_MedicalHistory": validate_dim_medical_history(df_medical_history),
        "Dim_Region": validate_dim_region(df_region),
        "Fact_Leukemia": validate_fact_leukemia(df_facts),
    }
