import dagster as dg
from dagster_aws.s3 import S3Resource
from src.iag.io_managers.pandas_parquet_io_mager import pandas_parquet_io_manager
from .resources import LocalsApiResource, PessoasResource
from ..resources import SqlAlchemyResource, ObfuscatorResource

LOCALS_API_URL = "https://www.telefones.iag.usp.br/export/rest"


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "pessoasinfo_replicado_db": SqlAlchemyResource(
                connection_string=dg.EnvVar("SQLSERVER_CONNECTION_STRING")
            ),
            "locals_api": LocalsApiResource(api_url=LOCALS_API_URL),
            "pessoasinfo_resources": PessoasResource(),
            "obfuscator": ObfuscatorResource(),
            "pessoasinfo_mysql_con": SqlAlchemyResource(
                connection_string=dg.EnvVar("MARIADB_CONNECTION_STRING")
            ),
            "s3_resource": S3Resource(
                endpoint_url=dg.EnvVar("AWS_S3_ENDPOINT"),
                aws_access_key_id=dg.EnvVar("AWS_ACCESS_KEY"),
                aws_secret_access_key=dg.EnvVar("AWS_SECRET_KEY"),
            ),
            "io_manager": pandas_parquet_io_manager,
        }
    )
