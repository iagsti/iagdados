import dagster as dg


from . import assets

alunosic_job = dg.define_asset_job(
    name="alunosic_job",
    selection=[
        assets.alunosic_raw,
        assets.alunosic_deduplicated,
        assets.alunosic_exceptions_aplyed,
        assets.alunosic_with_nomfnc,
        assets.alunosic_load,
    ],
)
