import dagster as dg
import pandas as pd
from ..resources import SqlAlchemyResource


@dg.asset()
def pessoa_raw(trino_resource: SqlAlchemyResource) -> pd.DataFrame:
    """
    Asset que representa a tabela 'pessoa' no banco de dados.

    :param trino_resource: Recurso para conexão com o banco de dados
    :return: DataFrame contendo os dados da tabela 'pessoa'
    """
    query = "SELECT * FROM pessoa_info"
    query = """
    SELECT
        codpes,
        nompes,
        codema,
        nomabvfnc,
    FROM alunoposinfo
    """
    engine = trino_resource.get_engine()
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df
