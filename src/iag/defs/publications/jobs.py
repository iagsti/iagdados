import dagster as dg


from . import assets


comprasgov_job = dg.define_asset_job(
    name="publications_job",
    selection=[

    ]
)