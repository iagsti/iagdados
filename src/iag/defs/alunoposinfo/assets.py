import re

import dagster as dg
import pandas as pd
from ..resources import SqlAlchemyResource, CleanerResource, IcebergResource
from .resources import AcessoResource


@dg.asset(kinds={"python", "pandas"})
def alunospos_raw_data(alunosposinfo_source: SqlAlchemyResource) -> pd.DataFrame:
    query = """
        WITH ALUNOSPOSATIVOS AS (
        SELECT 
        pessoa.codpes,
        pessoa.nompes,
        pessoa.sexpes,
        emailpessoa.codema,
        programa.nivpgm,
        programa.dtalimpgm,
        nomecurso.nomcur,
        orientador_pessoa.dtainiort,
        orientador_pessoa.dtafimort,
        orientador.nompes as orientador,
        ROW_NUMBER() OVER(PARTITION BY pessoa.codpes ORDER BY programa.numseqpgm  DESC) AS rn
        FROM ALUNOPOS alunopos
        INNER JOIN PESSOA AS pessoa ON pessoa.codpes = alunopos.codpes
        INNER JOIN EMAILPESSOA AS emailpessoa ON emailpessoa.codpes = alunopos.codpes AND emailpessoa.stamtr = 'S'
        INNER JOIN AGPROGRAMA AS programa ON programa.codpes = alunopos.codpes
        INNER JOIN AREA AS area ON area.codare = programa.codare
        INNER JOIN CURSO AS curso ON curso.codcur = area.codcur
        INNER JOIN NOMECURSO AS nomecurso ON nomecurso.codcur = curso.codcur
        INNER JOIN R39PGMORIDOC orientador_pessoa ON orientador_pessoa.codpespgm = alunopos.codpes AND orientador_pessoa.dtafimort IS NULL AND orientador_pessoa.tiport = 'ORI'
        INNER JOIN PESSOA AS orientador ON orientador.codpes = orientador_pessoa.codpes
        WHERE
        curso.codclg = 14
        )
        SELECT 
        a.*
        FROM ALUNOSPOSATIVOS  a
        WHERE 
        a.rn = 1 
        and a.codpes IN (SELECT DISTINCT l.codpes FROM LOCALIZAPESSOA l WHERE l.tipvin = 'ALUNOPOS')
        and a.nivpgm IS NOT NULL
        order by a.nompes
    """
    con = alunosposinfo_source.get_engine()
    alunospos_df = pd.read_sql(query, con=con)
    return alunospos_df


@dg.asset(kinds={"python", "pandas"})
def alunospos_cleaned(alunospos_raw_data: pd.DataFrame, alunosposinfo_cleaner: CleanerResource) -> pd.DataFrame:
    cleaned_df = alunosposinfo_cleaner.strip_columns(alunospos_raw_data)
    cleaned_df["nomcur"] = cleaned_df["nomcur"].replace({"Ensino de Astronomia": "Astronomia", "Mestrado Profissional Ensino de Astronomia": "Astronomia"})
    return cleaned_df


@dg.asset(kinds={"python", "pandas"})
def alunospos_formatted(alunospos_cleaned: pd.DataFrame) -> pd.DataFrame:
    cargo_mapping = {"M": "Aluno de Pós-Graduação", "F": "Aluna de Pós-Graduação"}
    formatted_df = pd.DataFrame()
    formatted_df["id"] = alunospos_cleaned["codpes"]
    formatted_df["nome"] = alunospos_cleaned["nompes"]
    formatted_df["num_usp"] = alunospos_cleaned["codpes"]
    formatted_df["cargo"] = alunospos_cleaned["sexpes"].apply(lambda x: cargo_mapping.get(x))
    formatted_df["email"] = alunospos_cleaned["codema"]
    formatted_df["departamento"] = alunospos_cleaned["nomcur"]
    formatted_df["instituicao"] = "IAG"
    formatted_df["responsavel"] = alunospos_cleaned["orientador"]
    return formatted_df


@dg.asset()
def alunospos_to_s3(context: dg.AssetExecutionContext ,alunospos_formatted: pd.DataFrame, iceberg_resource: IcebergResource):
    df = alunospos_formatted
    iceberg_resource.save(df=df, namespace="pessoas", table_name="alunospos_info", context=context)
    return df



