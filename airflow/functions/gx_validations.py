import great_expectations as gx
import pandas as pd

context = gx.get_context(mode="ephemeral")

def configure_gx():
    """
    Sets up the Great Expectations context with a pandas data source and batch definition.
    
    Returns:
        BatchDefinition: A batch definition for the context.
    """
    data_source = context.data_sources.add_pandas(name="pandas_source")
    data_asset = data_source.add_dataframe_asset(name="dataframe_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="batch_definition")
    return batch_definition

def setup_suite(suite_name):
    """
    Creates and registers an expectation suite with the given name.

    Args:
        suite_name: The name of the expectation suite.

    Returns:
        ExpectationSuite: The initialized and registered suite.
    """
    
    suite = gx.ExpectationSuite(name=suite_name)
    suite = context.suites.add(suite)
    return suite

def add_dim_patient_info_expectations(suite, df):
    """
    Adds expectations for the Dim_PatientInfo DataFrame, including identity and demographic checks.
    
    Raises:
        ValueError: If the DataFrame is None or empty.
    """

    if df is None or df.empty:
        raise ValueError("El DataFrame para Dim_PatientInfo está vacío o es None.")
    
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="patient_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="patient_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="patient_id"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="medical_history_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="medical_history_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="gender", value_set=["Male", "Female"]
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="age", min_value=0, max_value=110
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="socioeconomic_status", value_set=["Low", "Medium", "High"]
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="urban_rural", value_set=["Urban", "Rural"]
    ))
    return suite

def add_dim_medical_history_expectations(suite, df):
    """
    Adds expectations for the Dim_MedicalHistory DataFrame, validating IDs and binary fields.
    
    Raises:
        ValueError: If the DataFrame is None or empty.
    """

    if df is None or df.empty:
        raise ValueError("El DataFrame para Dim_MedicalHistory está vacío o es None.")
    
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="medical_history_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="medical_history_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="medical_history_id"))
    
    binary_cols = [
        "family_history", "smoking_status", "alcohol_consumption",
        "radiation_exposure", "infection_history", "chronic_illness",
        "immune_disorders", "genetic_mutation"
    ]
    
    for col in binary_cols:
        if col in df.columns:
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
                column=col, value_set=[0, 1]
            ))
    return suite

def add_dim_region_expectations(suite, df):
    """
    Adds expectations for the Dim_Region DataFrame, including ID, name, and format checks.
    
    Raises:
        ValueError: If the DataFrame is None or empty.
    """
    
    if df is None or df.empty:
        raise ValueError("El DataFrame para Dim_Region está vacío o es None.")
    
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="region_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="region_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="region_id"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="country"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="country"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
        column="country", regex=r"^[A-Za-z\s\-\.\']+$"
    ))
    return suite

def add_fact_leukemia_expectations(suite, df):
    """
    Adds expectations for the Fact_Leukemia DataFrame.

    Validates required columns, value types, nulls, and acceptable value ranges
    for medical and contextual features.
    
    Raises:
        ValueError: If the input DataFrame is None or empty.
    """

    if df is None or df.empty:
        raise ValueError("El DataFrame para Fact_Leukemia está vacío o es None.")
    
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="patient_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="region_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="leukemia_status", value_set=[0, 1]
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="living_status", value_set=[0, 1]
    ))
    
    float_columns = {
        "rbc_count": (0, 10),
        "hemoglobin_level": (4, 25),
        "bmi": (8, 50),
        "fertilizer_consumption": (0, None),
        "gdp_per_capita": (0, None),
        "pm25_pollution": (0, None),
        "undernourishment_rate": (0, 100),
        "agri_employment_pct": (0, 100),
        "co2_emissions_per_capita": (0, None),
        "nuclear_energy_pct": (0, 100),
        "alcohol_consumption_liters": (0, None)
    }
    
    for col, (min_val, max_val) in float_columns.items():
        if col in df.columns:
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
                column=col, type_="float"
            ))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=col))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
                column=col, min_value=min_val, max_value=max_val
            ))
    
    float_columns = {
        "wbc_count": (0, 50000),
        "platelet_count": (20000, 470000),
        "bone_marrow_blasts": (0, 100)
    }

    for col, (min_val, max_val) in float_columns.items():
        if col in df.columns:
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
                column=col, type_="int"
            ))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=col))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
                column=col, min_value=min_val, max_value=max_val
            ))

    return suite

def setup_validation (batch_definition, suite, validation_name):
    """
    Creates and registers a validation definition for a data batch.

    Args:
        batch_definition: Batch configuration for the validation.
        suite: Expectation suite to apply.
        validation_name: Name of the validation run.

    Returns:
        ValidationDefinition: Configured and registered validation definition.
    """

    validation_definition = gx.ValidationDefinition(
        data=batch_definition, suite=suite, name=validation_name
    )
    validation_definition = context.validation_definitions.add(validation_definition)
    return validation_definition

def run_validation(validation_definition, dataframe):
    """
    Executes a Great Expectations validation on a given DataFrame.

    Args:
        validation_definition: Configured validation object.
        dataframe: DataFrame to validate.

    Returns:
        dict: Validation result in boolean-only format.
    """

    batch_parameters = {"dataframe": dataframe}
    return validation_definition.run(
        batch_parameters=batch_parameters, 
        result_format="BOOLEAN_ONLY"
    )

def validate_all_dataframes(df):
    """
    Validates multiple DataFrames using predefined Great Expectations suites.

    Applies predefined expectation suites to each DataFrame, executes validations, 
    and returns the results for:
    - Dim_PatientInfo
    - Dim_MedicalHistory
    - Dim_Region
    - Fact_Leukemia

    Args:
        df: A dictionary containing the four DataFrames to validate, with keys:
        'Dim_PatientInfo', 'Dim_MedicalHistory', 'Dim_Region', 'Fact_Leukemia'.

    Returns:
        dict: A dictionary mapping each DataFrame name to its corresponding validation result."""
     
    batch_definition = configure_gx()
    
    expectation_functions = {
        "Dim_PatientInfo": add_dim_patient_info_expectations,
        "Dim_MedicalHistory": add_dim_medical_history_expectations,
        "Dim_Region": add_dim_region_expectations,
        "Fact_Leukemia": add_fact_leukemia_expectations
    }
    
    dataframes = {
        "Dim_PatientInfo": df['Dim_PatientInfo'],
        "Dim_MedicalHistory": df['Dim_MedicalHistory'],
        "Dim_Region": df['Dim_Region'],
        "Fact_Leukemia": df['Fact_Leukemia']
    }
    
    results = {}
    for df_name, dataframe in dataframes.items():
        suite = setup_suite(f"{df_name}_suite")
        
        expectation_functions[df_name](suite, dataframe)
        
        validation_definition = setup_validation(batch_definition, suite, f"{df_name}_validation" )
        results[df_name] = run_validation(validation_definition, dataframe)
    
    return results

def validation_results(df)-> bool:

    """
    Validate all DataFrames and return True if all validations pass, False otherwise.
    
    Args:
        df: Dictionary of {table_name: DataFrame} to validate
    
    Returns:
        bool: True if all validations pass, False if any fail
    """
    results = validate_all_dataframes(df)
    
    all_passed = all(result["success"] for result in results.values())
    return all_passed
