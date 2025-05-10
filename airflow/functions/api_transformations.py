import pandas as pd

def standardize_country_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes country names in the DataFrame based on a corrections dictionary.

    Args:
        df: Source DataFrame containing raw data.

    Returns:
        DataFrame with standardized country names.
    """
    corrections = {
    "usa": "united states",
    "uk": "united kingdom",
    "turkey": "turkiye",
    "russia": "russian federation",
    "south korea": "korea, rep." 
    } 

    df['Country'] = df['Country'].str.lower()
    df['Country'] = df['Country'].replace(corrections)
    return df


def filter_countries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the DataFrame to include only the needed countries.

    Args:
        df: Source DataFrame containing raw data.

    Returns:
        Filtered DataFrame containing only the needed countries.
    """

    needed_countries = ['argentina', 'australia', 'brazil', 'canada', 'china', 'france', 'germany', 'india', 'italy', 'japan', 'mexico', 'netherlands', 'norway', 'russia', 'saudi arabia', 'south africa', 'south korea', 'spain', 'sweden', 'turkey', 'uk', 'usa']

    return df[df['Country'].isin(needed_countries)]

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames columns in the DataFrame based on a dictionary of new column names.

    Args:
        df: Source DataFrame containing raw data.
    Returns:
        DataFrame with renamed columns.
    """
    rename_dict = {
    'Country': 'country',
    'Year': 'year',
    'Carbon dioxide (CO2) emissions per capita': 'co2_emissions_per_capita',
    'Electricity production from nuclear sources': 'nuclear_energy_pct',
    'Employment in agriculture': 'agri_employment_pct',
    'Fertilizer consumption': 'fertilizer_consumption',
    'GDP per capita': 'gdp_per_capita',
    'PM2.5 air pollution (mean annual exposure)': 'pm25_pollution',
    'Population living in slums': 'slum_population_pct',
    'Poverty headcount ratio at national poverty lines': 'poverty_rate',
    'Prevalence of moderate or severe food insecurity': 'food_insecurity_rate',
    'Prevalence of undernourishment': 'undernourishment_rate',
    'Total alcohol consumption per capita (liters of pure alcohol)': 'alcohol_consumption_liters'
    }

    return df.rename(columns=rename_dict)

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handles missing values in the DataFrame by filling with zeros or medians.

    Args:
        df: Source DataFrame containing raw data.

    Returns:
        DataFrame with missing values handled.
    """

    paises_sin_nuclear = ['saudi arabia', 'australia', 'norway', 'italy', 'turkiye']
    df.loc[(df['country'].isin(paises_sin_nuclear)) & (df['nuclear_energy_pct'].isna()), 'nuclear_energy_pct'] = 0

    df.loc[(df['country'] == 'germany') & (df['year'] > 2022), 'nuclear_energy_pct'] = 0

    df.loc[(df['country'] == 'saudi arabia') & (df['alcohol_consumption_liters'].isna()), 'alcohol_consumption_liters'] = 0

    few_nulls_vars = ['fertilizer_consumption', 'undernourishment_rate', 'pm25_pollution', 'alcohol_consumption_liters']

    for var in few_nulls_vars:
        df.loc[:, var] = df.groupby('country')[var].transform(
            lambda x: x.fillna(x.median()))

    return df


def drop_high_null_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops columns with a high percentage of null values.

    Args:
        df: Source DataFrame containing raw data.

    Returns:
        DataFrame with specified columns dropped.
    """
    return df.drop(columns=["slum_population_pct", "food_insecurity_rate", "poverty_rate"])


def impute_nuclear_energy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Imputes missing values in the 'nuclear_energy_pct' column using forward fill, backward fill, and interpolation.

    Args:
        df: Source DataFrame containing raw data.

    Returns:
        DataFrame with imputed 'nuclear_energy_pct' values.
    """
    df['nuclear_energy_pct'] = df.groupby('country')['nuclear_energy_pct'].ffill().bfill()
    df['nuclear_energy_pct'] = df.groupby('country')['nuclear_energy_pct'].transform(
        lambda x: x.interpolate(method='linear', limit_area='inside')
    )
    df['nuclear_energy_pct'] = df.groupby('country')['nuclear_energy_pct'].transform(
        lambda x: x.fillna(x.rolling(3, min_periods=1, center=True).mean())
    )
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes outliers from a specific column for a specific country using the IQR method.

    Args:
        df: Source DataFrame containing raw data.

    Returns:
        DataFrame with outliers removed.
    """
    Q1 = df[df['country'] == 'france']['fertilizer_consumption'].quantile(0.25)
    Q3 = df[df['country'] == 'france']['fertilizer_consumption'].quantile(0.75)
    IQR = Q3 - Q1
    upper_bound = Q3 + 1.5 * IQR

    df = df[~((df['country'] == 'france') & (df['fertilizer_consumption'] > upper_bound))]

def latest_year_data (df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the DataFrame to include only the latest year of data for each country.

    Args:
        df: Source DataFrame containing raw data.

    Returns:
        Filtered DataFrame with only the latest year of data for each country.
    """
    df = df[df['year'] == df.groupby('country')['year'].transform('max')]
    df = df.drop(columns=['year'])
    df = df.reset_index(drop=True)
    return df

def process_world_bank_data(df: pd.DataFrame) -> pd.DataFrame:

    df = standardize_country_names(df)
    df = filter_countries(df)
    df = rename_columns(df)
    df = handle_missing_values(df)
    df = drop_high_null_columns(df)
    df = impute_nuclear_energy(df)
    df = remove_outliers(df)
    df = latest_year_data(df)
    
    return df