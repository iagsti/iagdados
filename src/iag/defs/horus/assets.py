import dagster as dg
import pandas as pd
from datetime import datetime, timedelta
from .resources import HorusResource
from ..resources import SqlAlchemyResource, IcebergResource


DEFAULT_START_DATE = "2023/03/21"


def _logs_to_dataframe(log_list: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(log_list)
    if "placa" in df.columns:
        df["placa"] = df["placa"].astype("string")
    if "code_log" in df.columns:
        df["code_log"] = df["code_log"].astype("string")
    return df


def _last_log_date_to_dataframe(last_log_date: str) -> pd.DataFrame:
    df = pd.DataFrame([{"data": last_log_date}])
    return df

def _persist_last_log_date(iceberg_resource: IcebergResource, last_log_date: str, context: dg.AssetExecutionContext) -> None:
    last_log_date_df = _last_log_date_to_dataframe(last_log_date)
    iceberg_resource.save(
        df=last_log_date_df,
        namespace="horus",
        table_name="last_log_date",
        context=context,
        from_type="from_pandas",
        **{"preserve_index": False}
    )

@dg.asset(kinds={"python", "pandas", "trino"})
def last_log_date(context: dg.AssetExecutionContext, trino_resource: SqlAlchemyResource) -> pd.DataFrame:
    """Extrai a data do último log persistido, se houver."""
    try:
        engine = trino_resource.get_engine()
        query = """
            SELECT data
            FROM iceberg.horus.last_log_date
        """
        last_log = pd.read_sql(query, con=engine)
        context.log.info(last_log)
        return last_log
    except Exception:
        context.log.error("Error occurred while fetching last log date")
        return pd.DataFrame()

 
@dg.asset(kinds={"python", "pandas", "iceberg"})
def horus_log_historico_persisted(
    context: dg.AssetExecutionContext,
    horus_resource: HorusResource,
    iceberg_resource: IcebergResource,
    last_log_date: pd.DataFrame,
) -> None:
    last_date = last_log_date.iat[0, 0] if not last_log_date.empty else DEFAULT_START_DATE
    start_date = datetime.strptime(last_date, "%Y-%m-%d").date()
    start_date = start_date + timedelta(days=1)
    last_log_date = start_date.strftime("%Y/%m/%d")
    for date_item, log_list in horus_resource.iter_logs_since(start_date=start_date, context=context):
        last_log_date = date_item
        if log_list[0].get("status_code") and log_list[0]["status_code"] == 403:
            _persist_last_log_date(iceberg_resource, last_log_date, context)
            raise Exception(f"Access forbidden for date {date_item}. Check credentials or permissions.")
        if not log_list:
            continue
        df = _logs_to_dataframe(log_list)
        iceberg_resource.append(
            df,
            namespace="horus",
            table_name="log",
            context=context,
            from_type="from_pandas",
            **{"preserve_index": False},
        )
        context.log.info(f"Persisted {len(df)} logs for {date_item}")
    _persist_last_log_date(iceberg_resource, last_log_date, context)

