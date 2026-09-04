import re

import dagster as dg
import pandas as pd
from ..resources import SqlAlchemyResource, ObfuscatorResource, IcebergResource
from .resources import LocalsApiResource, PessoasResource


def get_pessoa_exceptions():
    return [
        {"codpes": 1253683, "codema": "paula.coelho@iag.usp.br"}
    ]


@dg.asset(kinds={"python", "pandas"})
def pessoasinfo_raw_data(pessoasinfo_replicado_db: SqlAlchemyResource) -> pd.DataFrame:
    query = """
        SELECT
        v.codpes,
        v.nompes,
        setor.nomabvset,
        setor.nomset,
        e.codema,
        p.sexpes,
        v.codset,
        v.tipvin,
        v.tipfnc,
        v.sitatl,
        v.nomabvcla,
        v.nomabvfnc
        from VINCULOPESSOAUSP v
        inner join EMAILPESSOA e ON e.codpes = v.codpes AND e.stamtr = 'S'
        inner join PESSOA p ON p.codpes = v.codpes
        inner join SETOR setor ON setor.codset = v.codset
        where v.codund = 14
        order by v.nompes
    """
    pessoas_df = pd.read_sql(query, con=pessoasinfo_replicado_db.get_engine())
    return pessoas_df


@dg.asset(kinds={"python", "pandas"})
def pessoasinfo_with_locals(
    locals_api: LocalsApiResource, pessoasinfo_raw_data: pd.DataFrame
) -> pd.DataFrame:
    locals_df = locals_api.get_locals()
    merged_df = pd.merge(
        pessoasinfo_raw_data, locals_df, how="left", left_on="codpes", right_on="codpes"
    )
    return merged_df


_RAMAL_PATTERN = re.compile(r"\b(\d{6})\b")
_RAMAL_MAPPER = {"91": "30", "48": "26", "56": "38"}


@dg.asset()
def pessoasinfo_with_telefone(
    context: dg.AssetExecutionContext, pessoasinfo_with_locals: pd.DataFrame
) -> pd.DataFrame:
    def build_telefone(ramal):
        if pd.isna(ramal):
            return None
        match = _RAMAL_PATTERN.search(str(ramal))
        if not match:
            return ramal
        r = match.group(1)
        prefix = _RAMAL_MAPPER.get(r[:2])
        return f"{prefix}{r}" if prefix else None

    df = pessoasinfo_with_locals.copy()
    df["telefone"] = df["ramal"].apply(build_telefone)
    context.log.info(
        f"Processed {df['telefone'].notna().sum()} telefones out of {len(df)}"
    )
    return df


@dg.asset(kinds={"python", "pandas"})
def pessoasinfo_with_ddd(
    context: dg.AssetExecutionContext,
    pessoasinfo_with_telefone: pd.DataFrame,
    pessoasinfo_resources: PessoasResource,
) -> pd.DataFrame:
    df = pessoasinfo_with_telefone.copy()
    df["ddd"] = df["ramal"].apply(
        lambda x: pessoasinfo_resources.get_ddd(context, x) if pd.notna(x) else None
    )
    return df


@dg.asset(
    kinds={"python", "pandas"},
    ins={"lattes_data": dg.AssetIn(key="lattes_link_dataframe")},
)
def pessoasinfo_with_lattes(
    lattes_data: pd.DataFrame, pessoasinfo_with_ddd: pd.DataFrame
) -> pd.DataFrame:
    df = pessoasinfo_with_ddd.merge(lattes_data, on="codpes", how="left")
    return df


@dg.asset(kinds={"python", "pandas"})
def pessoasinfo_with_sites(
    pessoasinfo_resources: PessoasResource, pessoasinfo_with_lattes: pd.DataFrame
) -> pd.DataFrame:
    sites = pessoasinfo_resources.get_sites()
    site_df = pd.DataFrame(sites)
    df = pessoasinfo_with_lattes.merge(site_df, on="codpes", how="left")
    return df


@dg.asset(kinds={"pandas", "python"})
def pessoasinfo_sanitized(
    pessoasinfo_resources: PessoasResource, pessoasinfo_with_sites: pd.DataFrame
) -> pd.DataFrame:
    pessoas_df = pessoasinfo_with_sites.copy()
    pessoas_df["nomfnc"] = pessoas_df["nomabvfnc"].str.strip()
    pessoas_df["sexpes"] = pessoas_df["sexpes"].str.strip()
    pessoas_df["nomabvset"] = pessoas_df["nomabvset"].str.strip()
    pessoas_df["tipvin"] = pessoas_df["tipvin"].str.strip()
    pessoas_df["nomfnc"] = pessoas_df["nomfnc"].str.replace(
        r"\bProf\b", "Professor", regex=True
    )
    for index, row in pessoas_df.iterrows():
        if row["sexpes"] == "F":
            pessoas_df.at[index, "nomfnc"] = pessoasinfo_resources.set_gender(
                row["nomfnc"]
            )
    return pessoas_df


@dg.asset(kinds={"pandas", "python"})
def pessoasinfo_professor_senior(pessoasinfo_replicado_db: SqlAlchemyResource):
    engine = pessoasinfo_replicado_db.get_engine()
    query = """
        SELECT *
        FROM VINCSATPROFSENIOR
        WHERE codund = 14
    """
    professores_df = pd.read_sql(query, con=engine)
    return professores_df


@dg.asset(kinds={"pandas", "python"})
def pessoasinfo_com_vinculo(
    pessoasinfo_professor_senior: pd.DataFrame, pessoasinfo_sanitized: pd.DataFrame
):
    ps = pessoasinfo_professor_senior.copy()
    ps_filtered = ps[ps["dtafimcbd"] >= pd.Timestamp.today().normalize()]
    ps_codpes = ps_filtered["codpes"]
    ps_codpes.drop_duplicates()
    df = pessoasinfo_sanitized.copy()
    aposentados = df[df["sitatl"] == "P"]
    sem_vinculo = aposentados[~aposentados["codpes"].isin(ps_codpes)]
    com_vinculo = df[~df["codpes"].isin(sem_vinculo["codpes"])]
    com_vinculo = com_vinculo[com_vinculo["sitatl"] != "D"]
    com_vinculo = com_vinculo.drop_duplicates()
    return com_vinculo


@dg.asset(kinds={"pandas", "python"})
def pessoasinfo_exceptions_aplyed(pessoasinfo_com_vinculo: pd.DataFrame):
    df = pessoasinfo_com_vinculo.copy()
    exceptions = get_pessoa_exceptions()
    for exception in exceptions:
        codpes = exception["codpes"]
        codema = exception["codema"]
        if codpes in df["codpes"].values:
            df.loc[df["codpes"] == codpes, "codema"] = codema
    return df


@dg.asset(kinds={"pandas", "python"})
def pessoasinfo_persisted_data(
    pessoasinfo_exceptions_aplyed: pd.DataFrame, pessoasinfo_mysql_con: SqlAlchemyResource
):
    df = pessoasinfo_exceptions_aplyed.copy()
    con = pessoasinfo_mysql_con.get_engine()
    df.to_sql(name="pessoas_info", con=con, if_exists="replace")
    return df


@dg.asset(kinds={"pandas", "minio", "iceberg"})
def pessoasinfo_s3_data(
    context: dg.AssetExecutionContext,
    pessoasinfo_com_vinculo: pd.DataFrame,
    iceberg_resource: IcebergResource,
):
    df = pessoasinfo_com_vinculo.copy()
    df["codpes"] = df["codpes"].astype(str)
    context.log.info(df.head())
    iceberg_resource.save(
        df,
        namespace="pessoas",
        table_name="pessoas_info",
        context=context,
        from_type="from_pandas",
        **{"preserve_index": False},
    )
    return df
