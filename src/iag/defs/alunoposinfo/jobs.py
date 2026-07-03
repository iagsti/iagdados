import dagster as dg


from . import assets

alunosposinfo_job = dg.define_asset_job(
    name="alunospos_job",
    selection=[
        assets.alunospos_raw_data,
        assets.alunospos_cleaned,
        assets.alunospos_formatted,
        assets.alunospos_loaded_to_acesso,
    ],
)
