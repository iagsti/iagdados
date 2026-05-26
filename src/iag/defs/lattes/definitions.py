import dagster as dg
from src.iag.io_managers.pandas_parquet_io_mager import pandas_parquet_io_manager
from .resources import LattesExtractorResource, LattesLinkTableResource
from ..resources import SqlAlchemyResource

SQLSERVER_CONNECTION_STRING = dg.EnvVar("SQLSERVER_CONNECTION_STRING")
MARIADB_CONNECTION_STRING = dg.EnvVar("MARIADB_CONNECTION_STRING")


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "replicado_db": SqlAlchemyResource(
                connection_string=SQLSERVER_CONNECTION_STRING
            ),
            "lattes_extractor": LattesExtractorResource(),
            "storage_db": SqlAlchemyResource(
                connection_string=MARIADB_CONNECTION_STRING
            ),
            "lattes_link_table": LattesLinkTableResource(),
            "io_manager": pandas_parquet_io_manager,
        }
    )
