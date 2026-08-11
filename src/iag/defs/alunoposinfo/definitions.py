import dagster as dg
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
        }
    )
