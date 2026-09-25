import dagster as dg


from . import assets


horus_job = dg.define_asset_job(
    name="horus_job",
    selection=[
        assets.last_log_date,
        assets.horus_log_bronze,
        assets.horus_log_bronze_bucket,
        assets.horus_log_bronze_data,
        assets.horus_log_bronze_deduplicated,
        assets.horus_log_bronze_normalized,
        assets.horus_log_silver_bucket,
        assets.horus_optimized_table,
        assets.horus_gold_aggr_month_week,
        assets.horus_gold_aggr_week_hour,
        assets.horus_gold_aggr_device_access,
        assets.horus_gold_aggr_tipo_cartao,
    ]
)