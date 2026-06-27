import dagster as dg
import pandas as pd
from ..resources import SqlAlchemyResource
from .resources import WordPressIngestionResource


@dg.asset()
def pessoas_raw(context: dg.AssetExecutionContext, siteiag_source: SqlAlchemyResource):
    query = """
    SELECT * FROM pessoas_info
    """
    con = siteiag_source.get_engine()
    df = pd.read_sql(query, con=con)
    msg = "Extraindo pessoas"
    context.log.info(msg=msg)
    return df


@dg.asset()
def corpo_funcional(context: dg.AssetExecutionContext, pessoas_raw: pd.DataFrame, siteiag_source: SqlAlchemyResource):
    msg = "Extraindo corpo funcional"
    context.log.info(msg=msg)
    corpo_funcional_df = pessoas_raw[
        (pessoas_raw["tipvin"] == "SERVIDOR") & 
        (~pessoas_raw["tipfnc"].str.contains("Docente", na=False)) &
        (~pessoas_raw["nomfnc"].str.contains("Prof", na=False))
    ]
    con = siteiag_source.get_engine()
    corpo_funcional_df.to_sql("corpo_funcional", con=con, if_exists="replace")
    return corpo_funcional_df


@dg.asset()
def persisted_corpo_funcional(
    context: dg.AssetExecutionContext,
    corpo_funcional: pd.DataFrame,
    wp_ingestion: WordPressIngestionResource
):
    msg = "Carregando corpo funcional"
    context.log.info(msg=msg)
    wp_ingestion.ingerir_dataframe(df=corpo_funcional, context=context)
    return corpo_funcional
