import dagster as dg
import pandas as pd
from ..resources import SqlAlchemyResource


def get_exceptions():
    alunosic_exceptions = [
        {"codpes": 11892820, "nompes": "Sora Satie Faria Nishimi"}
    ]
    alunosic_exceptions = pd.DataFrame(alunosic_exceptions).set_index("codpes")
    return alunosic_exceptions


@dg.asset(kinds={"python", "pandas"})
def alunosic_raw(alunosic_replicado_db: SqlAlchemyResource):
    query =  """
        SELECT distinct
        pessoa_aluno.codpes,
        pessoa_aluno.sexpes,
        pessoa_aluno.nompes,
        pessoa_orientador.nompes as 'orientador',
        projeto.staprj,
        projeto.dtafimprj,
        setor.nomset
        FROM ICTPROJETO projeto
        INNER JOIN PESSOA pessoa_aluno ON pessoa_aluno.codpes = projeto.codpesalu
        INNER JOIN PESSOA pessoa_orientador ON pessoa_orientador.codpes = projeto.codpesrsp
        INNER JOIN VINCULOPESSOAUSP vinculo ON vinculo.codpes = projeto.codpesrsp
        INNER JOIN SETOR setor ON setor.codset = vinculo.codset
        WHERE projeto.staprj = 'Ativo' AND setor.nomset IN ('Astronomia', 'Geofísica', 'Ciências Atmosféricas')
        ORDER BY pessoa_aluno.nompes
    """
    engine = alunosic_replicado_db.get_engine()
    df = pd.read_sql(query, con=engine)
    return df


@dg.asset(kinds={"python", "pandas"})
def alunosic_deduplicated(alunosic_raw: pd.DataFrame) -> pd.DataFrame:
    df = alunosic_raw.copy()
    df["dtafimprj_datetime"] = pd.to_datetime(df["dtafimprj"])
    df = df.sort_values("dtafimprj_datetime").drop_duplicates(subset=["codpes"], keep="last")
    return df


@dg.asset(kinds={"pandas", "python"})
def alunosic_exceptions_aplyed(alunosic_deduplicated: pd.DataFrame):
    df = alunosic_deduplicated.copy()
    exceptions = get_exceptions()
    df = df.set_index("codpes")
    df.update(exceptions)
    df = df.reset_index()
    return df


@dg.asset(kinds={"python", "pandas"})
def alunosic_with_nomfnc(alunosic_exceptions_aplyed: pd.DataFrame) -> pd.DataFrame:
    df = alunosic_exceptions_aplyed.copy()
    df["nomfnc"] = "Aluno de IC"
    return df


@dg.asset(kinds={"python", "pandas"})
def alunosic_load(alunosic_with_nomfnc: pd.DataFrame, alunosic_mysql_con: SqlAlchemyResource) -> pd.DataFrame:
    df = alunosic_with_nomfnc.copy()
    engine = alunosic_mysql_con.get_engine()
    df.to_sql("alunos_ic", con=engine, if_exists="append", index=False)
    return df