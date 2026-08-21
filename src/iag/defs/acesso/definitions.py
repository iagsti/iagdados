import dagster as dg
from .resources import AcessoResource
from ..resources import SqlAlchemyResource


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "acesso_resource": AcessoResource(
                api_url=dg.EnvVar("ACESSO_API_URL"),
                api_user=dg.EnvVar("ACESSO_API_USER"),
                api_password=dg.EnvVar("ACESSO_API_PASSWORD")
            ),
            "acesso_relational_db": SqlAlchemyResource(
                connection_string=dg.EnvVar("MARIADB_CONNECTION_STRING")
            )
        }
    )
