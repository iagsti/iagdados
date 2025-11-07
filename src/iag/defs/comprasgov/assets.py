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
    groups = catalog_groups.get_selected_groups()
    
    items_list = []

    for group in groups:
            page = 1
            has_more_pages = True
            while has_more_pages and not context.run.is_failure_or_canceled:
                try:
                    items = comprasgov_api.get_items(page=page, page_width=500, group_code=group)
                    items_list += (items.json()["resultado"])
                    has_more_pages = (items.json()["paginasRestantes"] > 0)
                    log_text = f"Appending page {page}, group {group}"
                    context.log.info(log_text)
                except:
                    context.log.info(items.url)
                    context.log.error(f"Can't get page {page}, group {group}")
                sleep(1)
                page = page + 1
    df = pd.DataFrame(items_list)
    return df


@dg.asset(kinds={"parquet"})
def raw_items_parquet(
    context: dg.AssetExecutionContext,
    raw_item_dataframe: pd.DataFrame
):
    filename = "raw_items"
    file_path = (Path(__file__).parent / f"../../data/raw/{filename}.parquet")
    raw_item_dataframe.to_parquet(file_path)
    return file_path


@dg.asset(kinds={"pandas"})
def items_keys_mapping(
    context: dg.AssetExecutionContext,
    raw_items_parquet
):
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


@dg.asset(kinds={"parquet"})
def silver_items_parquet(
    context: dg.AssetExecutionContext,
    items_keys_mapping: pd.DataFrame
):
    filename = "silver_items"
    file_path = (Path(__file__).parent / f"../../data/silver/{filename}.parquet")
    items_keys_mapping.to_parquet(file_path)
    return file_path


@dg.asset(kinds={"sqlalchemy", "pandas"})
def items_data_loading(
    sqlalchemy: resources.SqlAlchemyResource,
    items_keys_mapping: pd.DataFrame,
    comprasgov_table: resources.ComprasgovTableResource
):
    engine = sqlalchemy.get_engine()
    data = items_keys_mapping.to_dict(orient="records")
    ComprasGovTable = comprasgov_table.create_comprasgov_itens_table(engine=engine)

    with Session(engine) as session:
        session.bulk_insert_mappings(ComprasGovTable, data)
        session.commit()
