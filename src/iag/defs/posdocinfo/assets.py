import dagster as dg
import pandas as pd
from ..resources import SqlAlchemyResource


def _strip_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Remove espaços em branco no início e fim de colunas do tipo texto.
    """
    df = dataframe.copy()
    for column_name in df.columns:
        if pd.api.types.is_string_dtype(df[column_name]):
            df[column_name] = df[column_name].astype(str).str.strip()
    return df


@dg.asset(kinds={"sqlserver"})
def posdoc_bronze(replicado_resource: SqlAlchemyResource) -> pd.DataFrame:
    """Load the raw POSDOC data."""
    query = """
    SELECT
    DISTINCT
    projeto.staatlprj,
    pessoa_pd.codpes pdcodpes,
    pessoa_pd.nompes pdaluno,
    pessoa_pd.sexpes pdsexpes,
    email.codema,
    pessoa_sup.nompes supervisor,
    setor.nomset
    FROM PDPROJETO projeto
    INNER JOIN PDPROJETOSUPERVISOR supervisor ON supervisor.codprj = projeto.codprj AND supervisor.anoprj = projeto.anoprj
    INNER JOIN PESSOA as pessoa_sup ON pessoa_sup.codpes = supervisor.codpesspv
    INNER JOIN PESSOA as pessoa_pd ON pessoa_pd.codpes = projeto.codpes_pd
    INNER JOIN EMAILPESSOA as email ON email.codpes = pessoa_pd.codpes AND email.stamtr = 'S'
    INNER JOIN SETOR as setor ON setor.codset = projeto.codsetprj
    INNER JOIN VINCULOPESSOAUSP vinculo ON vinculo.codpes = pessoa_pd.codpes
    WHERE  vinculo.sitatl = 'A' AND projeto.staatlprj = 'Ativo'
    ORDER BY pessoa_pd.nompes;
    """
    connection = replicado_resource.get_engine()
    df = pd.read_sql(sql=query, con=connection)
    return df


@dg.asset()
def posdoc_silver(posdoc_bronze: pd.DataFrame) -> pd.DataFrame:
    df = posdoc_bronze.copy()
    cleaned_df = _strip_columns(df)
    cargo_mapping = {"M": "Doutorando", "F": "Doutoranda"}
    formatted_df = pd.DataFrame()
    formatted_df["id"] = cleaned_df["pdcodpes"]
    formatted_df["nome"] = cleaned_df["pdaluno"]
    formatted_df["num_usp"] = cleaned_df["pdcodpes"]
    formatted_df["cargo"] = cleaned_df["pdsexpes"].apply(lambda x: cargo_mapping.get(x))
    formatted_df["email"] = cleaned_df["codema"]
    formatted_df["departamento"] = cleaned_df["nomset"]
    formatted_df["instituicao"] = "IAG"
    formatted_df["responsavel"] = cleaned_df["supervisor"]
    return formatted_df


@dg.asset(kinds={"python", "pandas"})
def posdoc_gold(relational_resource: SqlAlchemyResource, posdoc_silver: pd.DataFrame) -> pd.DataFrame:
    engine = relational_resource.get_engine()
    posdoc_silver.to_sql("posdocinfo", con=engine, if_exists="replace", index=False)
    return posdoc_silver
