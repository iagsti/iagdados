import dagster as dg


from . import assets


enderecos_job = dg.define_asset_job(
    name="enderecos_job",
    selection=[
        assets.raw_enderecos_dataframe,
        assets.enderecos_table,
        assets.enderecos_geocoding_dataframe,
        assets.enderecos_geocoded_table,
    ]
)