import dagster as dg
from .resources import SqlAlchemyResource


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "mysql_resource": SqlAlchemyResource(connection_string=dg.EnvVar("MARIADB_CONNECTION_STRING")),
            "sqlserver_resource": SqlAlchemyResource(connection_string=dg.EnvVar("SQLSERVER_CONNECTION_STRING")),
        }
    )