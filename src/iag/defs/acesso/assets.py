import dagster as dg
import pandas as pd
from ..resources import SqlAlchemyResource


@dg.asset()
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
