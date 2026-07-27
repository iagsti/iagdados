import dagster as dg
from .resources import HorusResource
from ..resources import SqlAlchemyResource
from src.iag.io_managers.pandas_parquet_io_mager import pandas_parquet_io_manager


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "horus_resource": HorusResource(
                base_url=dg.EnvVar("HORUS_BASE_URL"), 
                username=dg.EnvVar("HORUS_USERNAME"),
                password=dg.EnvVar("HORUS_PASSWORD"),
                proxy_server="http://10.70.1.8"
            ),
            "horus_target": SqlAlchemyResource(
                connection_string=dg.EnvVar("MARIADB_CONNECTION_STRING")
            ),
            "io_manager": pandas_parquet_io_manager
        }
    )