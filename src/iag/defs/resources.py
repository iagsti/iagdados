import hashlib
import dagster as dg
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.table import TableProperties
from sqlalchemy import create_engine

# Tamanho-alvo de cada arquivo parquet gravado no lake. Menor que o padrão do
# pyiceberg (512 MiB) para que tabelas grandes sejam divididas em vários
# arquivos menores, reduzindo o tempo de cada upload individual ao S3 e
# evitando timeouts de rede (curlCode 28) em uploads muito longos.
WRITE_TARGET_FILE_SIZE_BYTES = 64 * 1024 * 1024  # 64 MB


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
            # Timeouts maiores para tolerar conexões lentas/instáveis durante
            # uploads grandes, evitando o erro NETWORK_CONNECTION (curlCode 28).
            "s3.connect-timeout": "60.0",
            "s3.request-timeout": "300.0",
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

    def _resolve_null_type(
        self, data_type: pa.DataType, reference_type: pa.DataType | None
    ) -> pa.DataType:
        """
        Substitui pa.null() (inclusive aninhado em list/struct, ex.: colunas de
        lista totalmente vazias viram list<null>) por um tipo concreto, já que o
        Iceberg (format-version 2) não aceita pa.null() em nenhum nível.
        """
        if pa.types.is_null(data_type):
            if reference_type is not None and not pa.types.is_null(reference_type):
                return reference_type
            return pa.string()
        if pa.types.is_list(data_type):
            reference_value_type = (
                reference_type.value_type
                if reference_type is not None and pa.types.is_list(reference_type)
                else None
            )
            new_value_type = self._resolve_null_type(data_type.value_type, reference_value_type)
            return data_type if new_value_type == data_type.value_type else pa.list_(new_value_type)
        if pa.types.is_struct(data_type):
            reference_fields = (
                {f.name: f.type for f in reference_type}
                if reference_type is not None and pa.types.is_struct(reference_type)
                else {}
            )
            new_fields, changed = [], False
            for field in data_type:
                new_type = self._resolve_null_type(field.type, reference_fields.get(field.name))
                changed = changed or new_type != field.type
                new_fields.append(pa.field(field.name, new_type))
            return pa.struct(new_fields) if changed else data_type
        return data_type

    def _resolve_null_types(
        self, arrow_table: pa.Table, reference_schema: pa.Schema | None
    ) -> pa.Table:
        """
        Colunas totalmente nulas viram pa.null() ao converter de pandas, tipo que
        o Iceberg (format-version 2) não aceita. Usa o tipo já existente na tabela
        para colunas conhecidas e cai para string em colunas novas.
        """
        reference_types = (
            {field.name: field.type for field in reference_schema}
            if reference_schema is not None
            else {}
        )
        fields, columns, changed = [], [], False
        for field in arrow_table.schema:
            column = arrow_table.column(field.name)
            target_type = self._resolve_null_type(field.type, reference_types.get(field.name))
            if target_type != field.type:
                column = column.cast(target_type)
                field = field.with_type(target_type)
                changed = True
            fields.append(field)
            columns.append(column)
        return pa.Table.from_arrays(columns, schema=pa.schema(fields)) if changed else arrow_table

    def _get_or_create_table(
        self,
        catalog: RestCatalog,
        namespace: str,
        table_name: str,
        arrow_table: pa.Table,
    ):
        identifier = (namespace, table_name)
        try:
            table = catalog.load_table(identifier)
            arrow_table = self._resolve_null_types(arrow_table, table.schema().as_arrow())
            return table, False, arrow_table
        except NoSuchTableError:
            arrow_table = self._resolve_null_types(arrow_table, None)
            table = catalog.create_table(
                identifier=identifier,
                schema=arrow_table.schema,
                properties={
                    TableProperties.WRITE_TARGET_FILE_SIZE_BYTES: str(
                        WRITE_TARGET_FILE_SIZE_BYTES
                    )
                },
            )
            return table, True, arrow_table

    def upsert_table(
        self,
        catalog: RestCatalog,
        namespace: str,
        table_name: str,
        df: any,
        from_type: str = "from_pandas",
        **kwargs: dict
    ) -> None:
        kwargs = {"preserve_index": False} if not kwargs else kwargs 
        arrow_table = getattr(pa.Table, from_type)(df, **kwargs)
        table, created, arrow_table = self._get_or_create_table(
            catalog, namespace, table_name, arrow_table
        )
        if created:
            table.append(arrow_table)
            return

        # Usar transação atômica evita o desalinhamento de metadados
        with table.transaction() as txn:
            with txn.update_schema() as schema_update:
                schema_update.union_by_name(arrow_table.schema)

            txn.set_properties(
                **{
                    TableProperties.WRITE_TARGET_FILE_SIZE_BYTES: str(
                        WRITE_TARGET_FILE_SIZE_BYTES
                    )
                }
            )
            txn.overwrite(arrow_table)

    def append_table(
        self,
        catalog: RestCatalog,
        namespace: str,
        table_name: str,
        df: any,
        from_type: str = "from_pandas",
        **kwargs: dict
    ) -> None:
        arrow_table = getattr(pa.Table, from_type)(df, **kwargs)
        table, created, arrow_table = self._get_or_create_table(
            catalog, namespace, table_name, arrow_table
        )
        if created:
            table.append(arrow_table)
            return

        # Usar transação atômica evita o desalinhamento de metadados
        with table.transaction() as txn:
            with txn.update_schema() as schema_update:
                schema_update.union_by_name(arrow_table.schema)

            txn.set_properties(
                **{
                    TableProperties.WRITE_TARGET_FILE_SIZE_BYTES: str(
                        WRITE_TARGET_FILE_SIZE_BYTES
                    )
                }
            )
            txn.append(arrow_table)

    def save(
        self,
        df: any,
        namespace: str,
        table_name: str,
        context: dg.AssetExecutionContext,
        from_type: str,
        warehouse: str = "lake",
        **kwargs: dict
    ) -> None:
        """
        Método principal — salva um DataFrame como tabela Iceberg, substituindo
        (overwrite) todo o conteúdo existente pelo snapshot recebido.
        """
        catalog = self.get_catalog(warehouse, context)
        self.ensure_namespace(catalog, namespace)
        self.upsert_table(catalog, namespace, table_name, df, from_type=from_type, **kwargs)

    def append(
        self,
        df: any,
        namespace: str,
        table_name: str,
        context: dg.AssetExecutionContext,
        from_type: str,
        warehouse: str = "lake",
        **kwargs: dict
    ) -> None:
        """
        Adiciona (append) um DataFrame a uma tabela Iceberg, preservando os dados
        já existentes — útil para cargas incrementais em lotes pequenos.
        """
        catalog = self.get_catalog(warehouse, context)
        self.ensure_namespace(catalog, namespace)
        self.append_table(catalog, namespace, table_name, df, from_type=from_type, **kwargs)