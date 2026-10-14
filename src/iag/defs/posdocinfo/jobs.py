import dagster as dg


from . import assets


posdoc_job = dg.define_asset_job(
    name="posdoc_job",
    selection=[
        assets.posdoc_bronze,
        assets.posdoc_silver,
        assets.posdoc_gold
    ]
)