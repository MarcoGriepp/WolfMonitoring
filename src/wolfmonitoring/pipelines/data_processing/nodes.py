from typing import List, Tuple, Callable
import pandas as pd

def groupby_aggregate(
    df: pd.DataFrame, 
    groupby_cols: List[str], 
    agg_instructions: List[Tuple[str, Callable]]
) -> pd.DataFrame:

    df_agg = df.groupby(groupby_cols).agg(
        {col: func for col, func in agg_instructions}).reset_index()
    return df_agg