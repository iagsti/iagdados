import dagster as dg
import pandas as pd
from time import sleep
from .resources import ContratosAPIResource
from ..resources import SqlAlchemyResource


@dg.asset(kinds={"pandas", "python"})
def raw_contratos_data(
    context: dg.AssetExecutionContext, contratos_api: ContratosAPIResource
) -> pd.DataFrame:
    """
    Extrai os dados de contratos do site: https://portalservicos.usp.br/contratacoes
    """
    raw_data_list = contratos_api.get_contratos(context=context)
    raw_data_dataframe = pd.DataFrame(raw_data_list)
    return raw_data_dataframe


@dg.asset(kinds={"pandas", "mysql", "python"})
def stored_raw_data(
    raw_contratos_data: pd.DataFrame, database_conn: SqlAlchemyResource
):
    """
    Armazena os dados de contratos no banco de dados.
    """
    name = "raw_contracts_data"
    conn = database_conn.get_engine()
    raw_contratos_data.to_sql(name=name, con=conn, if_exists="replace", index=False)
    return raw_contratos_data


@dg.asset(kinds={"pandas", "python"})
def raw_contratos_items(
    context: dg.AssetExecutionContext,
    contratos_api: ContratosAPIResource,
    raw_contratos_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Extrai os dados dos itens de cada contrato.
    """
    code_list = raw_contratos_data["codpcddsp"].to_list()
    items_list = []
    for codpcddsp in code_list:
        extracted_data = contratos_api.get_item(context=context, codpcddsp=codpcddsp)
        items_list.extend(extracted_data.get("items", []))
        sleep(2)
    raw_items_dataframe = pd.DataFrame(items_list)
    return raw_items_dataframe


@dg.asset(kinds={"pandas", "mysql"})
def stored_raw_contract_items_data(
    database_conn: SqlAlchemyResource, raw_contratos_items: pd.DataFrame
):
    """
    Armazena os itens dos contratos no banco de dados.
    """
    name = "raw_contracts_items"
    con = database_conn.get_engine()
    raw_contratos_items.to_sql(name=name, con=con, if_exists="replace", index=False)


@dg.asset(kinds={"pandas"})
def raw_contract_files(
    context: dg.AssetExecutionContext,
    contratos_api: ContratosAPIResource,
    raw_contratos_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Estrai os arquivos dos itens das contratações
    """
    code_list = raw_contratos_data["codpcddsp"].to_list()
    file_list = []
    for codpcddsp in code_list:
        extracted_data = contratos_api.get_files(codpcddsp=codpcddsp, context=context)
        file_list.extend(extracted_data.get("items", []))
    raw_contract_item_files_dataframe = pd.DataFrame(file_list)
    return raw_contract_item_files_dataframe


@dg.asset(kinds={"pandas"})
def contract_file_links(raw_contract_files: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona o link para os arquivos
    """
    files_list = raw_contract_files.copy()
    file_code_list = files_list["codarq_fs"].to_list()
    link_list = []
    base_link = "https://uspdigital.usp.br/wsinfo/arquivo?name=compraarq&codarq_fs="
    for code in file_code_list:
        link = f"{base_link}{code}"
        link_list.append(link)
    files_list["file_link"] = link_list
    return files_list


@dg.asset(kinds={"pandas"})
def stored_contract_files(
    contract_file_links: pd.DataFrame, database_conn: SqlAlchemyResource
):
    """
    Dados de arquivos armazenados no banco de dados.
    """
    name = "raw_contract_files"
    con = database_conn.get_engine()
    contract_file_links.to_sql(name=name, con=con, if_exists="replace", index=True)
