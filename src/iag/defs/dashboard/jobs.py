import dagster as dg


from . import assets


dashboard_job = dg.define_asset_job(
    name="dashboard_job",
    selection=[
        assets.raw_alunos,
        assets.alunos_persisted,
    ]
)