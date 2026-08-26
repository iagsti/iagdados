import dagster as dg
import pandas as pd
from .resources import HorusResource
from ..resources import SqlAlchemyResource, IcebergResource

DEFAULT_START_DATE = "2023/03/21"


@dg.asset(kinds={"python", "pandas", "trino"})
def last_log_date(trino_resource: SqlAlchemyResource) -> pd.DataFrame:
    """Extrai a data do último log persistido, se houver."""
    try:
        engine = trino_resource.get_engine()
        query = """
            SELECT data
            FROM iceberg.horus.log
            ORDER BY data desc
            LIMIT 1
        """
        return pd.read_sql(query, con=engine)
    except Exception:
        return pd.DataFrame()


def _logs_to_dataframe(log_list: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(log_list)
    if "placa" in df.columns:
        df["placa"] = df["placa"].astype("string")
    if "code_log" in df.columns:
        df["code_log"] = df["code_log"].astype("string")
    return df


@dg.asset(kinds={"python", "pandas", "iceberg"})
def horus_log_historico_persisted(
    context: dg.AssetExecutionContext,
    horus_resource: HorusResource,
    iceberg_resource: IcebergResource,
    last_log_date: pd.DataFrame,
) -> None:
    """
    Busca e persiste os logs dia a dia: cada dia vira um DataFrame pequeno que é
    enviado ao Iceberg imediatamente após a extração. Assim, se a extração falhar
    no meio do caminho, os dias já processados permanecem salvos.
    """
    start_date = last_log_date.iat[0, 0] if not last_log_date.empty else DEFAULT_START_DATE

    for date_item, log_list in horus_resource.iter_logs_since(start_date=start_date, context=context):
        if not log_list:
            continue
        df = _logs_to_dataframe(log_list)
        iceberg_resource.append(context=context, df=df, namespace="horus", table_name="log")
        context.log.info(f"Persisted {len(df)} logs for {date_item}")
