import dagster as dg
from .resources import OpenAlexResource
from ...resources import SqlAlchemyResource


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "openalex_db_source": SqlAlchemyResource(connection_string=dg.EnvVar("SQLSERVER_CONNECTION_STRING")),
            "openalex_resource": OpenAlexResource(
                openalex_key=dg.EnvVar("OPENALEX_KEY"),
                openalex_email=dg.EnvVar("OPENALEX_EMAIL"),
                per_page=dg.EnvVar.int("OPENALEX_PER_PAGE")
            )
        }
    )