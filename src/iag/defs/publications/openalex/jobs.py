import dagster as dg


from . import assets


openalex_job = dg.define_asset_job(
    name="openalex_job",
    selection=[
        assets.openalex_docentes,
        assets.openalex_docentes_cleaned_nompes,
        assets.openalex_articles,
        assets.openalex_persisted_articles
    ]
)