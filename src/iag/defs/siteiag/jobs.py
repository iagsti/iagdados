import dagster as dg


from . import assets


siteiag_job = dg.define_asset_job(
    name="siteiag",
    selection=[
        assets.pessoas_raw,
        assets.corpo_funcional,
        assets.persisted_corpo_funcional
    ]
)