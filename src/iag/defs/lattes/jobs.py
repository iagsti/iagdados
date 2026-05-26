import dagster as dg


from . import assets

lattes_job = dg.define_asset_job(
    name="lattes_job",
    selection=[
        assets.lattes_sql_raw_data,
        assets.lattes_data_without_duplicates,
        assets.lattes_link_dataframe,
        assets.lattes_persited_data,
    ],
)
