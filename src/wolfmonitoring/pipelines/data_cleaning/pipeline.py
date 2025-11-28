"""Data cleaning pipeline for wolf monitoring project."""
from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (concat_partitions, rename_columns, create_region_column)


def create_pipeline(**kwargs) -> Pipeline:
    """Create the data cleaning pipeline.

    Returns:
        A pipeline object
    """

    wolf_deaths_pipeline = pipeline(
        [
            node(
                func=concat_partitions, 
                inputs=["wolf_deaths", "params:wolf_deaths.data_cleaning.remove_duplicates"], 
                outputs="wolf_deaths_concatenated", 
                name="concat_partitions_deaths"
                ),


        ],
        tags=["data_cleaning"],
        parameters={
            "wolf_deaths.data_cleaning.remove_duplicates",
        }
    )


    wolf_damages_pipeline = pipeline(
        [
            node(
                func=concat_partitions, 
                inputs=["wolf_damages", "params:wolf_damages.data_cleaning.remove_duplicates"], 
                outputs="wolf_damages_concatenated", 
                name="concat_partitions_damages"
                ),
        ],
        tags=["data_cleaning"],
        parameters={
            "wolf_damages.data_cleaning.remove_duplicates",
        }
    )

    wolf_observations_pipeline = pipeline(
        [
            node(
                func=concat_partitions, 
                inputs=["wolf_observations", "params:wolf_observations.data_cleaning.remove_duplicates"], 
                outputs="wolf_observations_concatenated", 
                name="concat_partitions_observations"
                ),
            node(
                func=rename_columns,
                inputs=["wolf_observations_concatenated", "params:wolf_observations.data_cleaning.columns_mapping"],
                outputs="wolf_observations_renamed",
                name="rename_columns_observations"
            ),
            node(
                func=create_region_column,
                inputs=["wolf_observations_renamed", "params:wolf_observations.data_cleaning.region_mapping"],
                outputs="wolf_observations_cleaned",
                name="create_region_column_observations"
            ),
        ],
        tags=["data_cleaning"],
        parameters={
            "wolf_observations.data_cleaning.remove_duplicates",
            "wolf_observations.data_cleaning.columns_mapping",
            "wolf_observations.data_cleaning.region_mapping",
        }
    )

    data_cleaning_pipeline = (
        wolf_deaths_pipeline + wolf_damages_pipeline + wolf_observations_pipeline
    )

    return data_cleaning_pipeline