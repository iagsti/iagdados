import dagster as dg
import hashlib
import pandas as pd
import pyarrow as pa
from sqlalchemy import create_engine
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError


class SqlAlchemyResource(dg.ConfigurableResource):
    connection_string: str

    def get_engine(self):
        engine = create_engine(self.connection_string)
        return engine


class ObfuscatorResource(dg.ConfigurableResource):
    def obfuscate(self, codpes: int | str, length: int = 10):
        """
        Obfusca um código (int ou string) de forma determinística usando SHA-256.

        :param code: O código a ser ofuscado (int ou str)
        :param length: Tamanho do hash de saída (padrão: 10 caracteres)
        :return: String com o código ofuscado
        """
        if not isinstance(codpes, (str, int)):
            raise TypeError("O código deve ser uma string ou um número inteiro.")
        code_str = str(codpes)
        hash_obj = hashlib.sha256(code_str.encode())
        hash_hex = hash_obj.hexdigest()
        return hash_hex[:length]


class IcebergResource(dg.ConfigurableResource):
    lakekeeper_url: str
    aws_endpoint: str
    aws_access_key: str
    aws_secret_key: str
    aws_region: str

    def get_catalog(self, warehouse: str) -> RestCatalog:
        return RestCatalog(
            "lakekeeper",
            **{
                "uri": self.lakekeeper_url,
                "warehouse": warehouse,
                "s3.endpoint": self.aws_endpoint,
                "s3.access-key-id": self.aws_access_key,
                "s3.secret-access-key": self.aws_secret_key,
                "s3.path-style-access": "true",
                "s3.region": self.aws_region,
            },
        )

    def ensure_namespace(self, catalog: RestCatalog, namespace: str) -> None:
        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass

    def upsert_table(
        self,
        catalog: RestCatalog,
        warehouse: str,
        namespace: str,
        table_name: str,
        df: pd.DataFrame,
    ) -> None:
        """
        Cria a tabela se não existir, ou sobrescreve os dados se já existir.
        Idempotente — rodar duas vezes não duplica dados.
        """
        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        identifier = (namespace, table_name)

        try:
            table = catalog.load_table(identifier)
            table.overwrite(arrow_table)
        except NoSuchTableError:
            table = catalog.create_table(
                identifier=identifier,
                schema=arrow_table.schema,
                location=f"s3://{warehouse}/{namespace}/{table_name}",
            )
            table.append(arrow_table)

    def save(
        self,
        df: pd.DataFrame,
        namespace: str,
        table_name: str,
        warehouse: str = "lake",
    ) -> None:
        """
        Método principal — salva um DataFrame como tabela Iceberg.

        Uso:
            iceberg.save(df, namespace="analytics", table_name="pessoas_info")
        """
        catalog = self.get_catalog(warehouse)
        self.ensure_namespace(catalog, namespace)
        self.upsert_table(catalog, warehouse, namespace, table_name, df)
