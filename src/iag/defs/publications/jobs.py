import dagster as dg


from . import assets

publications_job = dg.define_asset_job(
    name="publications_job",
    selection=[
        assets.raw_lattes_data,
        assets.publications_sanitized_data,
        assets.publications_orcid_id,
        assets.publications_doi,
        assets.publications_with_orcid_and_doi,
        assets.publications_s3_data,
    ],
)
