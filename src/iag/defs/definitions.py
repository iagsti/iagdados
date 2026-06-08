import dagster as dg
from .resources import IcebergResource


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "iceberg_resource": IcebergResource(
                lakekeeper_url=dg.EnvVar("LAKEKEEPER_ENDPOINT"),
                aws_endpoint=dg.EnvVar("AWS_S3_ENDPOINT"),
                aws_access_key=dg.EnvVar("AWS_ACCESS_KEY"),
                aws_secret_key=dg.EnvVar("AWS_SECRET_KEY"),
                aws_region=dg.EnvVar("AWS_REGION"),
            )
        }
    )
