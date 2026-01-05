import pandas as pd
import dagster as dg
from .resources import SqlAlchemyResource


@dg.asset(kinds={"pandas"})
def raw_pessoas_dataframe(sqlserver_resource: SqlAlchemyResource) -> pd.DataFrame:
    engine = sqlserver_resource.get_engine()
    query = """
    SELECT
        DISTINCT
        localiza.nompes,
        vinculo.tipvin,
        vinculo.sitoco,
        FORMAT(vinculo.dtainisitoco, 'dd/MM/yyyy') inicio,
        FORMAT(vinculo.dtafimsitoco, 'dd/MM/yyyy') fim
        FROM LOCALIZAPESSOA as localiza
        INNER  JOIN VINCULOPESSOAUSP vinculo ON vinculo.codpes = localiza.codpes
        WHERE vinculo.tipvin = 'SERVIDOR' 
        AND localiza.codundclg = 14
        AND GETDATE() BETWEEN vinculo.dtainisitoco AND vinculo.dtafimsitoco 
        ORDER BY localiza.nompes
    """
    df = pd.read_sql(query, con=engine)
    return df


@dg.asset(kinds={"pandas"})
def pessoas_ferias_table(mysql_resource: SqlAlchemyResource, raw_pessoas_dataframe: pd.DataFrame) -> pd.DataFrame:
    engine = mysql_resource.get_engine()
    raw_pessoas_dataframe.to_sql(name='ferias', con=engine, if_exists='replace', index=False)
    return raw_pessoas_dataframe