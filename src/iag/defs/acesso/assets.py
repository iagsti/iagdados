import dagster as dg
import pandas as pd
from ..resources import SqlAlchemyResource
from .resources import AcessoResource


@dg.asset(kinds={"python", "pandas", "trino"})
def acesso_alunoposinfo(trino_resource: SqlAlchemyResource) -> pd.DataFrame:
    query = """
    SELECT
        nome,
        num_usp,
        cargo,
        departamento,
        instituicao,
        responsavel,
        email
    FROM iceberg.pessoas.alunospos_info
    """
    engine = trino_resource.get_engine()
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df


@dg.asset(kinds={"python", "pandas"})
def acesso_pessoapos(acesso_resource: AcessoResource):
    return acesso_resource.get_pessoaspos()


@dg.asset(kinds={"python", "pandas"})
def acesso_pessoapos_deleted(acesso_pessoapos: pd.DataFrame, acesso_alunoposinfo: pd.DataFrame):
    deleted_df = acesso_alunoposinfo[~acesso_alunoposinfo["num_usp"].isin(acesso_pessoapos["num_usp"])]
    deleted_df["deleted_at"] = pd.Timestamp.now()
    return deleted_df


@dg.asset(kinds={"python", "pandas"})
def acesso_upsert(acesso_resource: AcessoResource, acesso_alunoposinfo: pd.DataFrame):
    acesso_resource.upsert_pessoaspos(acesso_alunoposinfo)


@dg.asset(kinds={"python", "pandas"})
def acesso_soft_delete(acesso_resource: AcessoResource, acesso_pessoapos_deleted: pd.DataFrame):
    for _, row in acesso_pessoapos_deleted.iterrows():
        acesso_resource.soft_delete(row["num_usp"])


