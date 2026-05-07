import dagster as dg
from src.iag.io_managers.pandas_parquet_io_mager import pandas_parquet_io_manager
from .resources import ContratosAPIResource
from ..resources import SqlAlchemyResource


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "contratos_api": ContratosAPIResource(base_url=dg.EnvVar("CONTRATOS_API_BASE_URL")),
            "database_conn": SqlAlchemyResource(connection_string=dg.EnvVar("MARIADB_CONNECTION_STRING"))
        }
    )