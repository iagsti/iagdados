import dagster as dg


from . import assets

pessoasinfo_job = dg.define_asset_job(
    name="pessoasinfo_job",
    selection=[
        assets.pessoasinfo_raw_data,
        assets.pessoasinfo_with_locals,
        assets.pessoasinfo_with_telefone,
        assets.pessoasinfo_with_lattes,
        assets.pessoasinfo_with_sites,
        assets.pessoasinfo_sanitized,
        assets.pessoasinfo_professor_senior,
        assets.pessoasinfo_com_vinculo,
        assets.pessoasinfo_obfuscated,
        assets.pessoasinfo_persisted_data,
    ],
)
