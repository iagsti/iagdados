import dagster as dg


from . import assets


acesso_job = dg.define_asset_job(
    name="acesso_job",
    selection=[
        assets.acesso_alunoposinfo
    ]
)