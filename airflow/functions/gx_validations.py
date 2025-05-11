import great_expectations as gx
import pandas as pd

context = gx.get_context(mode="ephemeral")

def configure_gx():
    """Configure Great Expectations with a single context and pandas data source."""
    data_source = context.data_sources.add_pandas(name="pandas_source")
    data_asset = data_source.add_dataframe_asset(name="dataframe_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="batch_definition")
    return batch_definition

def setup_suite(suite_name):
    """Set  suite, and validation definition for a DataFrame."""
    
    suite = gx.ExpectationSuite(name=suite_name)
    suite = context.suites.add(suite)
    return suite

def add_dim_patient_info_expectations(suite, df):
    """Add expectations for Dim_PatientInfo DataFrame."""
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
    """Add expectations for Dim_MedicalHistory DataFrame."""
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
    """Add expectations for Dim_Region DataFrame."""
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
    """Add expectations for Fact_Leukemia DataFrame."""
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
    
    numeric_cols = [
        "wbc_countsort", "rbc_countsort", "platelet_countsort", "hemoglobin_level",
        "bone_marrow_blasts", "bmi", "fertilizer_consumption", "gdp_per_capita",
        "pm25_pollution", "undernourishment_grade", "agri_employment_pct",
        "co2_emissions_per_capita", "nuclear_energy_pct", "alcohol_consumption_liters"
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
                column=col, type_="float"
            ))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=col))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
                column=col, min_value=0
            ))
    return suite

def setup_validation (batch_definition, suite, validation_name):
    validation_definition = gx.ValidationDefinition(
        data=batch_definition, suite=suite, name=validation_name
    )
    validation_definition = context.validation_definitions.add(validation_definition)
    return validation_definition

def run_validation(validation_definition, dataframe):
    """Run validation on the provided dataframe."""
    batch_parameters = {"dataframe": dataframe}
    return validation_definition.run(
        batch_parameters=batch_parameters, 
        result_format="BOOLEAN_ONLY"
    )

def validate_all_dataframes(df):
    """Validate all four dataframes and return their results."""
    # Initialize single context and data source
    batch_definition = configure_gx()
    
    # Map DataFrame names to expectation functions
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
        # Set up validation for the current DataFrame
        suite = setup_suite(f"{df_name}_suite")
        
        # Add specific expectations
        expectation_functions[df_name](suite, dataframe)
        
        validation_definition = setup_validation(batch_definition, suite, f"{df_name}_validation" )
        # Run validation and store results
        results[df_name] = run_validation(validation_definition, dataframe)
    
    return results

def validation_results(df):

    results = validate_all_dataframes(df)
    for df_name, result in results.items():
        print(f"Validation results for {df_name}:")
        print(result)
