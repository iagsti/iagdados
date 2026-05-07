import dagster as dg
import pandas as pd
from .resources import ContratosAPIResource
from ..resources import SqlAlchemyResource


@dg.asset(kinds={"pandas"})
def raw_contratos_data(context: dg.AssetExecutionContext, contratos_api: ContratosAPIResource) -> pd.DataFrame:
    raw_data_list = contratos_api.get_contratos(context=context)
    raw_data_dataframe = pd.DataFrame(raw_data_list)
    return raw_data_dataframe


@dg.asset(kinds={"pandas", "mysql"})
def stored_raw_data(
    raw_contratos_data: pd.DataFrame,
    database_conn: SqlAlchemyResource    
):
    name = "raw_contratos_data"
    conn = database_conn.get_engine()
    raw_contratos_data.to_sql(name=name, con=conn, if_exists="replace", index=False)
