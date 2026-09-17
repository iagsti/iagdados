import dagster as dg
from ..resources import SqlAlchemyResource

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "alunosic_replicado_db": SqlAlchemyResource(connection_string=dg.EnvVar("SQLSERVER_CONNECTION_STRING")),
            "alunosic_mysql_con": SqlAlchemyResource(connection_string=dg.EnvVar("MARIADB_CONNECTION_STRING")),
        }
    )