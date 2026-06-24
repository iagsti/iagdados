import dagster as dg
from src.iag.io_managers.pandas_parquet_io_mager import pandas_parquet_io_manager
from ..resources import SqlAlchemyResource


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "alunos_target": SqlAlchemyResource(connection_string=dg.EnvVar("MARIADB_CONNECTION_STRING")),
            "alunos_source": SqlAlchemyResource(connection_string=dg.EnvVar("SQLSERVER_CONNECTION_STRING")),
            "io_manager": pandas_parquet_io_manager
        }
    )