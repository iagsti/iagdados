import dagster as dg
from .resources import (
    ComprasGovAPIResource,
    SqlAlchemyResource,
    CatalogGroupsResource,
    ComprasgovTableResource,
    PCATableResource,
    DataPathResource,
    MongoResource,
)


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "comprasgov_api": ComprasGovAPIResource(base_url=dg.EnvVar("COMPRASGOV_API_BASE_URL")),
            "sqlalchemy": SqlAlchemyResource(connection_string=dg.EnvVar("MARIADB_CONNECTION_STRING")),
            "engine_pca": SqlAlchemyResource(connection_string=dg.EnvVar("PCA_CONNECTION_STRING")),
            "catalog_groups": CatalogGroupsResource(),
            "comprasgov_table": ComprasgovTableResource(),
            "pca_table": PCATableResource(), 
            "data_path": DataPathResource(data_path="/opt/dagster/data"),
            "mongo_client": MongoResource(mongo_uri=dg.EnvVar("MONGO_DATABASE_URI"))
        }
    )