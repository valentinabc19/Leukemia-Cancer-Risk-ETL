import pandas as pd
from typing import Dict
from great_expectations.core import ExpectationSuiteValidationResult, ExpectationSuite
from great_expectations.validator.validator import Validator
from great_expectations.execution_engine.pandas_execution_engine import PandasExecutionEngine
from great_expectations.core.batch import Batch


def _get_validator(df: pd.DataFrame, suite_name: str) -> Validator:
    """
    Construye un Validator sin necesidad de un DataContext completo.

    Args:
        df: DataFrame de pandas a validar
        suite_name: Nombre del conjunto de expectativas

    Returns:
        Validator configurado para el DataFrame

    Raises:
        TypeError: Si df no es un DataFrame de pandas
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")

    engine = PandasExecutionEngine()

    batch = Batch(
        data=df
    )

    suite = ExpectationSuite(suite_name)

    return Validator(
        execution_engine=engine,
        batches=[batch],
        expectation_suite=suite,
    )


def validate_dim_patient_info(df: pd.DataFrame) -> ExpectationSuiteValidationResult:
    """
    Valida el DataFrame Dim_PatientInfo según reglas específicas.
    
    Args:
        df: DataFrame con datos de pacientes
        
    Returns:
        Resultado de la validación
        
    Raises:
        ValueError: Si el DataFrame está vacío o es None
    """
    if df is None or df.empty:
        raise ValueError("El DataFrame para Dim_PatientInfo está vacío o es None.")
    
    validator = _get_validator(df, "Dim_PatientInfo")

    # Validaciones básicas de identificación
    validator.expect_column_to_exist("patient_id")
    validator.expect_column_values_to_not_be_null("patient_id")
    validator.expect_column_values_to_be_unique("patient_id")
    
    # Validaciones de dominio
    validator.expect_column_values_to_be_in_set("gender", {"Male", "Female", "Other", "Unknown"})
    validator.expect_column_values_to_be_between("age", min_value=0, max_value=120)
    
    # Validaciones de categorías específicas
    validator.expect_column_values_to_be_in_set(
        "socioeconomic_status", 
        {"Low", "Medium", "High", "Unknown"}
    )
    validator.expect_column_values_to_be_in_set(
        "urban_rural", 
        {"Urban", "Rural", "Semi-Urban", "Unknown"}
    )

    return validator.validate()


def validate_dim_medical_history(df: pd.DataFrame) -> ExpectationSuiteValidationResult:
    """
    Valida el DataFrame Dim_MedicalHistory según reglas específicas.
    
    Args:
        df: DataFrame con historiales médicos
        
    Returns:
        Resultado de la validación
        
    Raises:
        ValueError: Si el DataFrame está vacío o es None
    """
    if df is None or df.empty:
        raise ValueError("El DataFrame para Dim_MedicalHistory está vacío o es None.")
    
    validator = _get_validator(df, "Dim_MedicalHistory")

    # Validaciones de identificador único
    validator.expect_column_to_exist("medical_history_id")
    validator.expect_column_values_to_not_be_null("medical_history_id")
    validator.expect_column_values_to_be_unique("medical_history_id")
    
    # Validaciones de relaciones
    validator.expect_column_to_exist("patient_id")
    validator.expect_column_values_to_not_be_null("patient_id")

    # Columnas binarias
    binary_cols = {
        "family_history", "smoking_status", "alcohol_consumption", 
        "radiation_exposure", "infection_history", "chronic_illness", 
        "immune_disorders", "genetic_mutation"
    }
    
    # Solo validar columnas que existen en el DataFrame
    existing_binary_cols = [col for col in binary_cols if col in df.columns]
    
    for col in existing_binary_cols:
        validator.expect_column_values_to_be_in_set(col, {0, 1})

    
    return validator.validate()


def validate_dim_region(df: pd.DataFrame) -> ExpectationSuiteValidationResult:
    """
    Valida el DataFrame Dim_Region según reglas específicas.
    
    Args:
        df: DataFrame con datos regionales
        
    Returns:
        Resultado de la validación
        
    Raises:
        ValueError: Si el DataFrame está vacío o es None
    """
    if df is None or df.empty:
        raise ValueError("El DataFrame para Dim_Region está vacío o es None.")

    validator = _get_validator(df, "Dim_Region")

    # Validaciones de identificador único
    validator.expect_column_to_exist("region_id")
    validator.expect_column_values_to_not_be_null("region_id")
    validator.expect_column_values_to_be_unique("region_id")
    
    # Validaciones de país
    validator.expect_column_to_exist("country")
    validator.expect_column_values_to_not_be_null("country")
    validator.expect_column_values_to_match_regex("country", r"^[A-Za-z\s\-\.]+$")

    return validator.validate()


def validate_fact_leukemia(df: pd.DataFrame) -> ExpectationSuiteValidationResult:
    """
    Valida el DataFrame Fact_Leukemia según reglas específicas.
    
    Args:
        df: DataFrame con datos de leucemia
        
    Returns:
        Resultado de la validación
        
    Raises:
        ValueError: Si el DataFrame está vacío o es None
    """
    if df is None or df.empty:
        raise ValueError("El DataFrame para Fact_Leukemia está vacío o es None.")

    validator = _get_validator(df, "Fact_Leukemia")

    # Validaciones de relaciones
    validator.expect_column_to_exist("patient_id")
    validator.expect_column_values_to_not_be_null("patient_id")
    validator.expect_column_to_exist("region_id")
    validator.expect_column_values_to_not_be_null("region_id")
    
    # Validaciones de estado
    validator.expect_column_values_to_be_in_set("leukemia_status", {0, 1})  # 2 para indeterminado
    validator.expect_column_values_to_be_in_set("living_status", {0, 1})    # 9 para desconocido

    # Definición de columnas numéricas con sus rangos esperados
    numeric_columns = {
        "wbc_count": (0, 100),
        "rbc_count": (0, 10),
        "platelet_count": (0, 450000),
        "hemoglobin_level": (4, 25),
        "bone_marrow_blasts": (0, 100),
        "bmi": (10, 50),
        "fertilizer_consumption": (0, None),
        "gdp_per_capita": (0, None),
        "pm25_pollution": (0, None),
        "undernourishment_rate": (0, 100),
        "agri_employment_pct": (0, 100),
        "co2_emissions_per_capita": (0, None),
        "nuclear_energy_pct": (0, 100),
        "alcohol_consumption_liters": (0, None)
    }
    
    # Validar solo columnas existentes
    for col, (min_val, max_val) in numeric_columns.items():
        if col in df.columns:
            validator.expect_column_values_to_be_of_type(col, "float")
            validator.expect_column_values_to_not_be_null(col)
            validator.expect_column_values_to_be_between(
                col, 
                min_value=min_val, 
                max_value=max_val
            )

    return validator.validate()


def validate_all(
    dataframes: Dict[str, pd.DataFrame]
) -> Dict[str, ExpectationSuiteValidationResult]:
    """
    Valida todos los DataFrames según sus validadores específicos.
    
    Args:
        dataframes: Diccionario de DataFrames a validar
        custom_validators: Diccionario opcional con validadores personalizados
        required_tables: Conjunto de tablas requeridas (si no se proporcionan, se genera error)
        
    Returns:
        Diccionario con resultados de validación
        
    Raises:
        ValueError: Si faltan tablas requeridas o hay tablas no reconocidas
    """
    # Validadores por defecto
    default_validators = {
        "Dim_PatientInfo": validate_dim_patient_info,
        "Dim_MedicalHistory": validate_dim_medical_history,
        "Dim_Region": validate_dim_region,
        "Fact_Leukemia": validate_fact_leukemia,
    }
    
    # Combinar con validadores personalizados
    validators = {**default_validators}
    
    # Ejecutar validaciones
    results = {}
    for table_name, df in dataframes.items():
        if table_name in validators:
            try:
                results[table_name] = validators[table_name](df)
            except Exception as e:
                results[table_name] = str(e)
    
    return results