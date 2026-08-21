import hashlib
import dagster as dg
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from sqlalchemy import create_engine


class SqlAlchemyResource(dg.ConfigurableResource):
    connection_string: str

    def get_engine(self):
        return create_engine(self.connection_string)


class ObfuscatorResource(dg.ConfigurableResource):
    def obfuscate(self, codpes: int | str, length: int = 10) -> str:
        """
        Ofusca um código (int ou string) de forma determinística usando SHA-256.
        """
        if not isinstance(codpes, (str, int)):
            raise TypeError("O código deve ser uma string ou um número inteiro.")
        code_str = str(codpes)
        hash_hex = hashlib.sha256(code_str.encode()).hexdigest()
        return hash_hex[:length]


class CleanerResource(dg.ConfigurableResource):
    def strip_columns(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Remove espaços em branco no início e fim de colunas do tipo texto.
        """
        df = dataframe.copy()
        for column_name in df.columns:
            if pd.api.types.is_string_dtype(df[column_name]):
                df[column_name] = df[column_name].astype(str).str.strip()
        return df


class IcebergResource(dg.ConfigurableResource):
    lakekeeper_url: str
    aws_endpoint: str
    aws_access_key: str
    aws_secret_key: str
    aws_region: str

    def get_catalog(
        self, warehouse: str, context: dg.AssetExecutionContext
    ) -> RestCatalog:
        props = {
            "uri": self.lakekeeper_url,
            "warehouse": warehouse,
            "s3.endpoint": self.aws_endpoint,
            "s3.access-key-id": self.aws_access_key,
            "s3.secret-access-key": self.aws_secret_key,
            "s3.path-style-access": "true",
            "s3.region": self.aws_region,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.request-checksum-calculation": "when_required",
            "s3.response-checksum-validation": "when_required",
        }
        
        # Log seguro Omitindo credenciais sensíveis
        safe_props = {
            k: ("***" if "secret" in k or "key" in k else v)
            for k, v in props.items()
        }
        context.log.info(f"Conectando ao catálogo Iceberg com props: {safe_props}")
        
        return RestCatalog("lakekeeper", **props)

    def ensure_namespace(self, catalog: RestCatalog, namespace: str) -> None:
        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass

    def upsert_table(
        self,
        catalog: RestCatalog,
        namespace: str,
        table_name: str,
        df: pd.DataFrame,
    ) -> None:
        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        identifier = (namespace, table_name)
    
        try:
            table = catalog.load_table(identifier)
        except NoSuchTableError:
            table = catalog.create_table(
                identifier=identifier,
                schema=arrow_table.schema,
            )
            table.append(arrow_table)
            return
    
        # Usar transação atômica evita o desalinhamento de metadados
        with table.transaction() as txn:
            with txn.update_schema() as schema_update:
                schema_update.union_by_name(arrow_table.schema)
            
            txn.overwrite(arrow_table)

    def save(
        self,
        df: pd.DataFrame,
        namespace: str,
        table_name: str,
        context: dg.AssetExecutionContext,
        warehouse: str = "lake",
    ) -> None:
        """
        Método principal — salva um DataFrame como tabela Iceberg.
        """
        catalog = self.get_catalog(warehouse, context)
        self.ensure_namespace(catalog, namespace)
        self.upsert_table(catalog, namespace, table_name, df)