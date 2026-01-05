import dagster as dg


from . import assets


pessoas_job = dg.define_asset_job(
    name="pessoas_job",
    selection=[
        assets.raw_pessoas_dataframe,
        assets.pessoas_ferias_table
    ]
)