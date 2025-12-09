import dagster as dg


from . import assets


comprasgov_job = dg.define_asset_job(
    name="comprasgov_job",
    selection=[
        assets.raw_item_dataframe,
        assets.raw_price_dataframe,
        assets.raw_price_parquet,
        assets.raw_items_parquet,
        assets.items_keys_mapping,
        assets.items_without_duplicates,
        assets.existing_items,
        assets.no_existing_items,
        assets.silver_items_parquet,
        assets.items_data_loading,
        assets.items_pca_data_options,
        assets.items_to_mongo,
    ]
)