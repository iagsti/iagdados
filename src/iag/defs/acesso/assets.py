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
def acesso_pessoapos_deleted(
    context: dg.AssetExecutionContext,
    acesso_pessoapos: pd.DataFrame,
    acesso_alunoposinfo: pd.DataFrame,
) -> pd.DataFrame:
    for name, df in (("acesso_pessoapos", acesso_pessoapos), ("acesso_alunoposinfo", acesso_alunoposinfo)):
        if "num_usp" not in df.columns:
            context.log.error(
                f"{name} veio sem a coluna 'num_usp'. shape={df.shape} colunas={list(df.columns)}"
            )
            raise ValueError(f"{name} sem a coluna 'num_usp' (colunas: {list(df.columns)})")

    # num_usp vem como int64 do Trino e como string da API do Acesso — normaliza pra comparar
    pessoapos_ids = acesso_pessoapos["num_usp"].astype(str)
    alunoposinfo_ids = acesso_alunoposinfo["num_usp"].astype(str)

    deleted_df = acesso_pessoapos[~pessoapos_ids.isin(alunoposinfo_ids)].copy()
    deleted_df["deleted_at"] = pd.Timestamp.now()
    return deleted_df


@dg.asset(kinds={"python", "pandas"})
def acesso_upsert(acesso_resource: AcessoResource, acesso_alunoposinfo: pd.DataFrame):
    acesso_resource.upsert_pessoaspos(acesso_alunoposinfo)


@dg.asset(kinds={"python", "pandas"})
def acesso_soft_delete(acesso_resource: AcessoResource, acesso_pessoapos_deleted: pd.DataFrame):
    acesso_resource.upsert_pessoaspos(acesso_pessoapos_deleted)


