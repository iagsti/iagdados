import dagster as dg
from src.iag.io_managers.pandas_parquet_io_mager import pandas_parquet_io_manager
from ..resources import SqlAlchemyResource
from .resources import WordPressIngestionResource


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "siteiag_source": SqlAlchemyResource(connection_string=dg.EnvVar("MARIADB_CONNECTION_STRING")),
            "siteiag_target": SqlAlchemyResource(connection_string=dg.EnvVar("SITEIAG_MARIADB_CONNECTION_STRING")),
            "wp_ingestion": WordPressIngestionResource(api_url=dg.EnvVar("WP_IAG_SITE_URL"), username=dg.EnvVar("WP_IAG_USERNAME"), password=dg.EnvVar("WP_IAG_PASSWORD")),
            "io_manager": pandas_parquet_io_manager
        }
    )