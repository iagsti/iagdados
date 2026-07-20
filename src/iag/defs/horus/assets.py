import dagster as dg
import pandas as pd
from time import sleep
from .resources import HorusResource
from ..resources import SqlAlchemyResource


@dg.asset()
def last_log_date(horus_target: SqlAlchemyResource) -> pd.DataFrame:
    """
    Extract last date log update
    """
    try:
        con = horus_target.get_engine()
        query = """
            SELECT data
            FROM horus_historico
            ORDER BY data desc
            LIMIT 1
        """
        last_date = pd.read_sql(query, con=con)
        return last_date
    except Exception:
        return pd.DataFrame()


@dg.asset()
def horus_log_historico(context: dg.AssetExecutionContext, horus_resource: HorusResource, last_log_date: pd.DataFrame):
    last_date = last_log_date.iat[0, 0] if not last_log_date.empty else "2023/03/21"
    date_list = horus_resource.get_date_list(initial_date=last_date)
    log_list = []
    token = horus_resource.get_token()
    last_date_parts = last_date.split("/")
    year = int(last_date_parts[0])
    month = int(last_date_parts[1])
    day = int(last_date_parts[2])
    for date_item in date_list:
        date_interval = horus_resource.calc_date_interval(ano_inicio=year, mes_inicio=month, dia_inicio=day)
        context.log.info(f"Processing date {date_item}")
        logs = horus_resource.update_logs(date_interval=date_interval, limit=67, token=token, data=date_item, context=context)
        context.log.info(logs)
        log_list.extend(logs)
        sleep(2)
    df = pd.DataFrame(log_list)
    return df


@dg.asset()
def horus_log_historico_persisted(horus_target: SqlAlchemyResource, horus_log_historico: pd.DataFrame):
    engine = horus_target.get_engine()
    horus_log_historico.to_sql("horus_historico", con=engine, if_exists="append", index=False)
    