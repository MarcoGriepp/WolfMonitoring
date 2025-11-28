import pandas as pd
from typing import List, Tuple, Callable

def concat_partitions(partitions: dict, remove_duplicates: bool = False) -> pd.DataFrame:

    concatenated = []
    
    for _, dataset_load_func in partitions.items():
        # 1. Call the load function to get the actual data for the partition
        partition_data = dataset_load_func()
        
        # 2. Append the loaded data (DataFrame) to the list
        concatenated.append(partition_data)

    concatenated_df = pd.concat(concatenated, ignore_index=True)
    if remove_duplicates:
        return concatenated_df.drop_duplicates().reset_index(drop=True)
    return concatenated_df

def rename_columns(df: pd.DataFrame, columns_mapping: dict) -> pd.DataFrame:

    return df.rename(columns=columns_mapping)

def create_region_column(df: pd.DataFrame, region_mapping: dict) -> pd.DataFrame:
    """Create a 'region' column based on existing location data.

    Args:
        df: Input DataFrame with location data.
        region_mapping: A dictionary mapping location identifiers to regions.

    Returns:
        DataFrame with an added 'region' column.
    """
    df['region'] = df['location'].map(region_mapping)
    return df

