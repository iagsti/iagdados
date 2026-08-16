import dagster as dg
from ..resources import SqlAlchemyResource, CleanerResource


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "alunosposinfo_source": SqlAlchemyResource(connection_string=dg.EnvVar("SQLSERVER_CONNECTION_STRING")),
            "alunosposinfo_cleaner": CleanerResource() ,
        }
    )
