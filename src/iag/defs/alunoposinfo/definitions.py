import dagster as dg
from src.iag.io_managers.pandas_parquet_io_mager import pandas_parquet_io_manager
from .resources import AcessoResource
from ..resources import SqlAlchemyResource, CleanerResource


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "alunosposinfo_source": SqlAlchemyResource(connection_string=dg.EnvVar("SQLSERVER_CONNECTION_STRING")),
            "acesso_resource": AcessoResource(
                api_url=dg.EnvVar("ACESSO_API_URL"),
                api_user=dg.EnvVar("ACESSO_API_USER"),
                api_password=dg.EnvVar("ACESSO_API_PASSWORD")
            ),
            "alunosposinfo_cleaner": CleanerResource() ,
            "io_manager": pandas_parquet_io_manager
        }
    )
