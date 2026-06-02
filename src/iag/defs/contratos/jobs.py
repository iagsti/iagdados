import dagster as dg


from . import assets


contratos_job = dg.define_asset_job(
    name="contratos_job",
    selection=[
        assets.raw_contratos_data,
        assets.stored_raw_data,
        assets.raw_contratos_items,
        assets.stored_raw_contract_items_data,
        assets.raw_contract_files,
        assets.contract_file_links,
        assets.stored_contract_files,
    ]
)