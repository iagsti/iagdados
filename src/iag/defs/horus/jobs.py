import dagster as dg


from . import assets


horus_job = dg.define_asset_job(
    name="horus_job",
    selection=[
        assets.last_log_date,
        assets.horus_log_historico_persisted,
    ]
)