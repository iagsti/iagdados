import dagster as dg
from .resources import CorssrefApiResource, TesesUspResource, OrcidApiResource
from ...resources import SqlAlchemyResource


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "replicado_con": SqlAlchemyResource(connection_string=dg.EnvVar("SQLSERVER_CONNECTION_STRING")),
            "crossref_api": CorssrefApiResource(base_url="https://api.crossref.org/works"),
            "publications_db_target": SqlAlchemyResource(connection_string=dg.EnvVar("MARIADB_CONNECTION_STRING")),
            "teses_resource": TesesUspResource(unidade="IAG"),
            "orcid_api": OrcidApiResource(client_id=dg.EnvVar("ORCID_CLIENT_ID"), client_secret=dg.EnvVar("ORCID_CLIENT_SECRET"), sandbox=False),
        }
    )