import dagster as dg
import pandas as pd
from ..resources import SqlAlchemyResource
from .resources import LattesExtractorResource, LattesLinkTableResource


@dg.asset(kinds={"python", "pandas", "sqlserver"})
def lattes_sql_raw_data(replicado_db: SqlAlchemyResource) -> pd.DataFrame:
    query = """
        SELECT
        l.nompes,
        dpx.* 
        from DIM_PESSOA_XMLUSP dpx
        inner join LOCALIZAPESSOA l on l.codpes = dpx.codpes
        where l.codundclg = 14
        order by l.nompes
    """
    lattes_df = pd.read_sql(query, con=replicado_db.get_engine())
    return lattes_df


@dg.asset(kinds={"python", "pandas"})
def lattes_data_without_duplicates(lattes_sql_raw_data: pd.DataFrame) -> pd.DataFrame:
    lattes_data = lattes_sql_raw_data.drop_duplicates(subset=["codpes"])
    return lattes_data


@dg.asset(kinds={"python", "pandas"})
def lattes_link_dataframe(
    context: dg.AssetExecutionContext,
    lattes_extractor: LattesExtractorResource,
    lattes_data_without_duplicates: pd.DataFrame,
) -> pd.DataFrame:
    lattes_data = []
    for _, row in lattes_data_without_duplicates.iterrows():
        lattes_link = lattes_extractor.extract_xml_content(row["imgarqxml"], context)
        if lattes_link:
            item = {"codpes": row["codpes"], "lattes_link": lattes_link}
            lattes_data.append(item)
    lattes_data_df = pd.DataFrame(lattes_data)
    return lattes_data_df


@dg.asset(kinds={"python", "pandas"})
def lattes_persited_data(
    lattes_link_dataframe: pd.DataFrame,
    storage_db: SqlAlchemyResource,
    lattes_link_table: LattesLinkTableResource,
) -> pd.DataFrame:
    df = lattes_link_dataframe.copy()
    engine = storage_db.get_engine()
    lattes_link_table.create_table(engine)
    df.to_sql("lattes_links", con=engine, if_exists="append", index=False)
    return df
