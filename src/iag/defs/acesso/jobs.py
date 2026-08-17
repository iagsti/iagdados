import dagster as dg


from . import assets


acesso_job = dg.define_asset_job(
    name="acesso_job",
    selection=[
        assets.acesso_alunoposinfo,
        assets.acesso_pessoapos,
        assets.acesso_pessoapos_deleted,
        assets.acesso_upsert,
        assets.acesso_soft_delete,
    ]
)