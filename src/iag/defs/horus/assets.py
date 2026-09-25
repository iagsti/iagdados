import dagster as dg
import pandas as pd
from datetime import datetime, timedelta
from .resources import HorusResource
from ..resources import SqlAlchemyResource, IcebergResource


# DEFAULT_START_DATE = "2023/03/21"
DEFAULT_START_DATE = "2026/09/18"

def _logs_to_dataframe(log_list: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(log_list)
    if "placa" in df.columns:
        df["placa"] = df["placa"].astype("string")
    if "code_log" in df.columns:
        df["code_log"] = df["code_log"].astype("string")
    return df


@dg.asset(kinds={"python", "pandas", "trino"})
def last_log_date(context: dg.AssetExecutionContext, trino_resource: SqlAlchemyResource) -> pd.DataFrame:
    """Extrai a data do último log persistido, se houver."""
    try:
        engine = trino_resource.get_engine()
        query = """
            SELECT MAX(data)
            FROM iceberg.horus.log
            LIMIT 1
        """
        last_log = pd.read_sql(query, con=engine)
        context.log.info(last_log)
        return last_log
    except Exception:
        context.log.error("Error occurred while fetching last log date")
        return pd.DataFrame()

 
@dg.asset(kinds={"python", "pandas"})
def horus_log_bronze(
    context: dg.AssetExecutionContext,
    horus_resource: HorusResource,
    last_log_date: pd.DataFrame,
) -> pd.DataFrame:
    last_date = last_log_date.iat[0, 0] if not last_log_date.empty else DEFAULT_START_DATE
    last_date_datetime = datetime.strptime(last_date, "%Y/%m/%d").date()
    last_date_datetime = last_date_datetime + timedelta(days=1)
    logs = horus_resource.iter_logs_since(start_date=last_date_datetime, context=context)
    logs_df = _logs_to_dataframe(logs)
    return logs_df


@dg.asset(kinds={"pandas", "iceberg"})
def horus_log_bronze_bucket(
    context: dg.AssetExecutionContext,
    iceberg_resource: IcebergResource,
    horus_log_bronze: pd.DataFrame
):
    iceberg_resource.append(
        df=horus_log_bronze,
        namespace=("horus",),
        table_name="log_bronze",
        context=context,
        from_type="from_pandas",
        **{"preserve_index": False}
    )
    context.log.info(f"Perssited {horus_log_bronze.count()} logs")
    return horus_log_bronze


@dg.asset()
def horus_log_bronze_data(trino_resource: SqlAlchemyResource, horus_log_bronze_bucket: pd.DataFrame):
    query = """
        SELECT
            data,
            hora,
            ip,
            numserchi,
            nroevento,
            evento,
            tipopessoa,
            nomepessoa,
            codpes,
            nrodoc,
            tipodoc,
            local,
            nroporta,
            descricaoporta,
            devicename,
            tipocartao,
            vinculousp,
            code_log,
            endpoint
        FROM iceberg.horus.log_bronze
    """
    engine = trino_resource.get_engine()
    bronze_data_df = pd.read_sql(sql=query, con=engine)
    return bronze_data_df


@dg.asset()
def horus_log_bronze_deduplicated(horus_log_bronze_data: pd.DataFrame):
    deduplicated_data_df = horus_log_bronze_data.drop_duplicates(subset=['hora', 'codpes'])
    return deduplicated_data_df


@dg.asset()
def horus_log_bronze_normalized(horus_log_bronze_deduplicated: pd.DataFrame):
    normalized_df = horus_log_bronze_deduplicated.copy()
    normalized_df["nomepessoa"] = normalized_df["nomepessoa"].str.title()
    return normalized_df


@dg.asset()
def horus_log_silver_bucket(
    context: dg.AssetExecutionContext,
    iceberg_resource: IcebergResource,
    horus_log_bronze_normalized: pd.DataFrame,
):
    iceberg_resource.append(
        df=horus_log_bronze_normalized,
        namespace=("horus",),
        table_name="log_silver",
        context=context,
        from_type="from_pandas",
        **{"preserve_index": False}
    )
    return horus_log_bronze_normalized


@dg.asset(kinds={"trino"})
def horus_optimized_table(trino_resource: SqlAlchemyResource, horus_log_silver_bucket: pd.DataFrame):
    query_optimize = """
        ALTER TABLE iceberg.horus.{level}
        EXECUTE OPTMIZE (target_file_size => '256MB')
    """

    query_minifest = """
        ALTER TABLE iceberg.horus.{level}
        EXECUTE REWRITE_MANIFESTS;
    """
    engine = trino_resource.get_engine()
    pd.read_sql(sql=query_optimize.format(level="log_bronze"), con=engine)
    pd.read_sql(sql=query_minifest.format(level="log_bronze"), con=engine)
    pd.read_sql(sql=query_optimize.format(level="log_silver"), con=engine)
    pd.read_sql(sql=query_minifest.format(level="log_silver"), con=engine)


@dg.asset()
def horus_gold_aggr_month_week(
    context: dg.AssetExecutionContext,
    horus_optimized_table: pd.DataFrame,
    trino_resource: SqlAlchemyResource,
    iceberg_resource: IcebergResource
):
    query = """
        WITH dados_formatados AS (
            SELECT
                month(date_parse(data, '%Y/%m/%d')) AS mes_num,
                day_of_week(date_parse(data, '%Y/%m/%d')) AS dia_num
            FROM iceberg.horus.log_silver
        )
        SELECT
            CASE mes_num
                WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
                WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho'
                WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
                WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
            END AS mes,
            CASE dia_num
                WHEN 1 THEN 'Segunda' WHEN 2 THEN 'Terça' WHEN 3 THEN 'Quarta'
                WHEN 4 THEN 'Quinta' WHEN 5 THEN 'Sexta' WHEN 6 THEN 'Sábado' WHEN 7 THEN 'Domingo'
            END AS dia_semana,
            COUNT(*) AS total_acessos
        FROM dados_formatados
        GROUP BY mes_num, dia_num
        ORDER BY mes_num, dia_num;
    """
    engine = trino_resource.get_engine()
    df = pd.read_sql(sql=query, con=engine)
    iceberg_resource.append(
        df=df,
        namespace=("horus",),
        table_name="gold_aggr_month_week",
        context=context,
        **{"preserve_index": False}
    )


@dg.asset()
def horus_gold_aggr_week_hour(
    context: dg.AssetExecutionContext,
    horus_optimized_table: pd.DataFrame,
    trino_resource: SqlAlchemyResource,
    iceberg_resource: IcebergResource
):
    query = """
        WITH dados_formatados AS (
            SELECT
                day_of_week(date_parse(data, '%Y/%m/%d')) AS dia_num,
                CAST(substr(hora, 1, 2) AS INTEGER) AS hora_num
            FROM iceberg.horus.log_silver  -- <--- Troque pelo nome real!
        )
        SELECT
            CASE dia_num
                WHEN 1 THEN 'Segunda'
                WHEN 2 THEN 'Terça'
                WHEN 3 THEN 'Quarta'
                WHEN 4 THEN 'Quinta'
                WHEN 5 THEN 'Sexta'
                WHEN 6 THEN 'Sábado'
                WHEN 7 THEN 'Domingo'
            END AS dia_semana,
            hora_num,
            COUNT(*) AS total_acessos
        FROM dados_formatados
        GROUP BY dia_num, hora_num
        ORDER BY dia_num, hora_num;
    """
    engine = trino_resource.get_engine()
    df = pd.read_sql(sql=query, con=engine)
    iceberg_resource.append(
        df=df,
        namespace=("horus",),
        table_name="gold_aggr_week_hour",
        context=context,
        **{"preserve_index": False}
    )


@dg.asset()
def horus_gold_aggr_device_access(
    context: dg.AssetExecutionContext,
    horus_optimized_table: pd.DataFrame,
    trino_resource: SqlAlchemyResource,
    iceberg_resource: IcebergResource
):
    query = """
        SELECT 
        devicename,
        COUNT(devicename) acessos
        FROM iceberg.horus.log_silver
        GROUP BY devicename
    """
    engine = trino_resource.get_engine()
    df = pd.read_sql(sql=query, con=engine)
    iceberg_resource.append(
        df=df,
        namespace=("horus",),
        table_name="gold_aggr_device_access",
        context=context,
        **{"preserve_index": False}
    )


@dg.asset()
def horus_gold_aggr_tipo_cartao(
    context: dg.AssetExecutionContext,
    horus_optimized_table: pd.DataFrame,
    trino_resource: SqlAlchemyResource,
    iceberg_resource: IcebergResource
):
    query = """
        SELECT 
        tipocartao,
        COUNT(tipocartao) quantidade
        FROM iceberg.horus.log_silver
        WHERE tipocartao IS NOT NULL
        GROUP BY tipocartao;
    """
    engine = trino_resource.get_engine()
    df = pd.read_sql(sql=query, con=engine)
    iceberg_resource.append(
        df=df,
        namespace=("horus",),
        table_name="gold_aggr_tipo_cartao",
        context=context,
        **{"preserve_index": False}
    )