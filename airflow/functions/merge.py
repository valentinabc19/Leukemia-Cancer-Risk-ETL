import pandas as pd

def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Merges two dataframes on 'country' columns. 
    If 'country' is not present in both dataframes, it raises a ValueError.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
    Returns:
        Merged DataFrame
    Raises:
        ValueError: If 'country' column is not present in both DataFrames
    """
    if 'country' not in df1.columns or 'country' not in df2.columns:
        raise ValueError("Both DataFrames must contain a 'country' column")
    
    merged_df = pd.merge(df1, df2, on='country', how='inner')
    return merged_df