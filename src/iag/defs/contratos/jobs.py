import dagster as dg


from . import assets


contratos_job = dg.define_asset_job(
    name="contratos_job",
    selection=[
        assets.raw_contratos_data,
        assets.stored_raw_data,
    ]
)