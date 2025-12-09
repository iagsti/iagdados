import pandas as pd
import dagster as dg
from time import sleep
from pathlib import Path
from . import resources
from sqlalchemy.orm import Session


@dg.asset(kinds={"pandas"})
def raw_item_dataframe(
    context: dg.AssetExecutionContext,
    comprasgov_api: resources.ComprasGovAPIResource,
    catalog_groups: resources.CatalogGroupsResource
) -> pd.DataFrame:
    """
    Extrai os dados de  items
    """
    groups = catalog_groups.get_selected_groups()
    df = comprasgov_api.extract_data(
        context=context,
        reference_list=groups,
        resource_name="get_items",
        page_width=500
    )
    return df


@dg.asset(kinds={"python", "pandas"})
def raw_price_dataframe(
    context: dg.AssetExecutionContext,
    comprasgov_api: resources.ComprasGovAPIResource,
    raw_item_dataframe: pd.DataFrame
):
    codigo_item_list = raw_item_dataframe["codigoItem"].to_list()
    price_list = comprasgov_api.extract_data(
        context=context,
        reference_list=codigo_item_list,
        resource_name="get_preco",
        page_width=500
    )
    df = pd.DataFrame(price_list)
    return df


@dg.asset(kinds={"pandas"})
def raw_price_parquet(
    context: dg.AssetExecutionContext,
    data_path: resources.DataPathResource,
    raw_price_dataframe: pd.DataFrame
):
    filename = "raw_price"
    path = data_path.get_data_path()
    file_path = f"{path}/raw/{filename}.parquet"
    context.log.info(f"Gravando dados em {file_path}")
    raw_price_dataframe.to_parquet(file_path)
    return file_path


@dg.asset(kinds={"parquet"})
def raw_items_parquet(
    context: dg.AssetExecutionContext,
    data_path: resources.DataPathResource,
    raw_item_dataframe: pd.DataFrame
):
    filename = "raw_items"
    path = data_path.get_data_path()
    file_path = f"{path}/raw/{filename}.parquet"
    context.log.info(f"Gravando dados em {file_path}")
    raw_item_dataframe.to_parquet(file_path)
    return file_path


@dg.asset(kinds={"pandas"})
def items_keys_mapping(
    context: dg.AssetExecutionContext,
    raw_items_parquet
):
    context.log.info("Mapeando dados")
    items_df = pd.read_parquet(raw_items_parquet)
    keys_mapping = {
        "codigoItem": "codigo_item",
        "codigoGrupo": "codigo_grupo",
        "nomeGrupo": "nome_grupo",
        "codigoClasse": "codigo_classe",
        "nomeClasse": "nome_classe",
        "codigoPdm": "codigo_pdm",
        "nomePdm": "nome_pdm",
        "descricaoItem": "descricao_item",
        "statusItem": "status_item",
        "itemSustentavel": "item_sustentavel",
        "descricaoNcm": "descricao_ncm",
        "dataHoraAtualizacao": "data_hora_atualizacao"
    }
    renamed_df = items_df.rename(columns=keys_mapping)
    return renamed_df


@dg.asset(kinds={"pandas"})
def items_without_duplicates(items_keys_mapping: pd.DataFrame):
    items_no_duplicates = items_keys_mapping.drop_duplicates(
        subset=["codigo_item"],
        keep="first"
    ).reset_index(drop=True)
    return items_no_duplicates


@dg.asset(kinds={"sqlalchemy", "pandas"})
def existing_items(engine_pca: resources.SqlAlchemyResource):
    engine = engine_pca.get_engine()
    query = "SELECT codigo_item FROM core_item"
    existing_data_df = pd.read_sql(query, engine)
    return existing_data_df


@dg.asset(kinds={"pandas"})
def no_existing_items(
    existing_items: pd.DataFrame,
    items_without_duplicates: pd.DataFrame
):
    no_existing_df = items_without_duplicates[
        ~items_without_duplicates["codigo_item"].isin(existing_items["codigo_item"])
    ]
    return no_existing_df


@dg.asset(kinds={"parquet"})
def silver_items_parquet(
    context: dg.AssetExecutionContext,
    data_path: resources.DataPathResource,
    items_without_duplicates: pd.DataFrame
):
    filename = "silver_items"
    path = data_path.get_data_path()
    file_path = f"{path}/silver/{filename}.parquet"
    items_without_duplicates.to_parquet(file_path)
    return file_path


@dg.asset(kinds={"sqlalchemy", "pandas"})
def items_data_loading(
    sqlalchemy: resources.SqlAlchemyResource,
    items_without_duplicates: pd.DataFrame,
    comprasgov_table: resources.ComprasgovTableResource
):
    engine = sqlalchemy.get_engine()
    data = items_without_duplicates.to_dict(orient="records")
    ComprasGovTable = comprasgov_table.create_comprasgov_itens_table(engine=engine)

    with Session(engine) as session:
        session.bulk_insert_mappings(ComprasGovTable, data)
        session.commit()


@dg.asset(kinds={"sqlalchemy"})
def items_pca_data_options(
    engine_pca: resources.SqlAlchemyResource,
    no_existing_items: pd.DataFrame,
    pca_table: resources.PCATableResource
):
    engine = engine_pca.get_engine()
    selected_columns = [
        "codigo_grupo",
        "nome_grupo",
        "codigo_classe",
        "nome_classe",
        "codigo_pdm",
        "nome_pdm",
        "codigo_item",
        "descricao_item",
    ]
    columns = no_existing_items[selected_columns]
    data = columns.to_dict(orient="records")
    CoreItemTable = pca_table.create_pca_itens_table(engine=engine)

    with Session(engine) as session:
        session.bulk_insert_mappings(CoreItemTable, data)
        session.commit()


@dg.asset(kinds={"mongodb", "pandas"})
def items_to_mongo(
    no_existing_items: pd.DataFrame,
    mongo_client: resources.MongoResource
):
    client = mongo_client.get_client()
    db = client["pca"]
    collection = db["core_items"]
    selected_columns = [
        "codigo_grupo",
        "nome_grupo",
        "codigo_classe",
        "nome_classe",
        "codigo_pdm",
        "nome_pdm",
        "codigo_item",
        "descricao_item",
    ]
    columns = no_existing_items[selected_columns]
    data = columns.to_dict(orient="records")
    collection.insert_many(data)