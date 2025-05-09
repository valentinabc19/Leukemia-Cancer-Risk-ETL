def standardize_country_names(df: pd.DataFrame, corrections: Dict[str, str]) -> pd.DataFrame:
    """
    Standardizes country names in the DataFrame based on a corrections dictionary.

    Args:
        df: Source DataFrame containing raw data.
        corrections: Dictionary mapping incorrect country names to standardized names.

    Returns:
        DataFrame with standardized country names.
    """
    df['Country'] = df['Country'].str.lower()
    df['Country'] = df['Country'].replace(corrections)
    return df


def filter_countries(df: pd.DataFrame, needed_countries: List[str]) -> pd.DataFrame:
    """
    Filters the DataFrame to include only the needed countries.

    Args:
        df: Source DataFrame containing raw data.
        needed_countries: List of countries to keep in the DataFrame.

    Returns:
        Filtered DataFrame containing only the needed countries.
    """
    return df[df['Country'].isin(needed_countries)]


def handle_missing_values(df: pd.DataFrame, zero_fill_columns: List[str], median_fill_columns: List[str]) -> pd.DataFrame:
    """
    Handles missing values in the DataFrame by filling with zeros or medians.

    Args:
        df: Source DataFrame containing raw data.
        zero_fill_columns: List of columns where missing values should be filled with zeros.
        median_fill_columns: List of columns where missing values should be filled with the median.

    Returns:
        DataFrame with missing values handled.
    """
    for col in zero_fill_columns:
        df[col] = df[col].fillna(0)

    for col in median_fill_columns:
        df[col] = df.groupby('Country')[col].transform(lambda x: x.fillna(x.median()))

    return df


def drop_high_null_columns(df: pd.DataFrame, columns_to_drop: List[str]) -> pd.DataFrame:
    """
    Drops columns with a high percentage of null values.

    Args:
        df: Source DataFrame containing raw data.
        columns_to_drop: List of column names to drop.

    Returns:
        DataFrame with specified columns dropped.
    """
    return df.drop(columns=columns_to_drop)


def impute_nuclear_energy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Imputes missing values in the 'nuclear_energy_pct' column using forward fill, backward fill, and interpolation.

    Args:
        df: Source DataFrame containing raw data.

    Returns:
        DataFrame with imputed 'nuclear_energy_pct' values.
    """
    df['nuclear_energy_pct'] = df.groupby('Country')['nuclear_energy_pct'].ffill().bfill()
    df['nuclear_energy_pct'] = df.groupby('Country')['nuclear_energy_pct'].transform(
        lambda x: x.interpolate(method='linear', limit_area='inside')
    )
    df['nuclear_energy_pct'] = df.groupby('Country')['nuclear_energy_pct'].transform(
        lambda x: x.fillna(x.rolling(3, min_periods=1, center=True).mean())
    )
    return df


def remove_outliers(df: pd.DataFrame, column: str, country: str) -> pd.DataFrame:
    """
    Removes outliers from a specific column for a specific country using the IQR method.

    Args:
        df: Source DataFrame containing raw data.
        column: Column name where outliers should be removed.
        country: Country name for which outliers should be removed.

    Returns:
        DataFrame with outliers removed.
    """
    Q1 = df[df['Country'] == country][column].quantile(0.25)
    Q3 = df[df['Country'] == country][column].quantile(0.75)
    IQR = Q3 - Q1
    upper_bound = Q3 + 1.5 * IQR

    return df[~((df['Country'] == country) & (df[column] > upper_bound))]

def process_world_bank_data(df: pd.DataFrame, corrections: Dict[str, str], needed_countries: List[str]) -> pd.DataFrame:
    df = standardize_country_names(df, corrections)
    df = filter_countries(df, needed_countries)
    df = handle_missing_values(df, zero_fill_columns=['nuclear_energy_pct'], median_fill_columns=['fertilizer_consumption'])
    df = drop_high_null_columns(df, columns_to_drop=['slum_population_pct', 'poverty_rate', 'food_insecurity_rate'])
    df = impute_nuclear_energy(df)
    df = remove_outliers(df, column='fertilizer_consumption', country='france')
    return df