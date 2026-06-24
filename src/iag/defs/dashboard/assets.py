import dagster as dg
import pandas as pd
from ..resources import SqlAlchemyResource


@dg.asset()
def raw_alunos(alunos_source: SqlAlchemyResource):
    query = """
    SELECT
    YEAR(pg.dtaing)             AS ano_ingresso,
    hb.codcur                   AS cod_curso,
    cg.nomcur                   AS nome_curso,
    pg.tiping                   AS tipo_ingresso,
    COUNT(DISTINCT pg.codpes)   AS qtd_ingressantes
    FROM PROGRAMAGR pg
    INNER JOIN HABILPROGGR hb ON hb.codpes = pg.codpes AND hb.codpgm = pg.codpgm
    INNER JOIN CURSOGR cg ON cg.codcur = hb.codcur
    WHERE pg.dtaing IS NOT NULL
    AND hb.dtafim IS NULL
    GROUP BY
    YEAR(pg.dtaing),
    hb.codcur,
    cg.nomcur,
    pg.tiping
    ORDER BY ano_ingresso, qtd_ingressantes DESC;
    """
    pessoas_df = pd.read_sql(query, con=alunos_source.get_engine())
    return pessoas_df


@dg.asset()
def alunos_persisted(raw_alunos: pd.DataFrame, alunos_target: SqlAlchemyResource):
    con = alunos_target.get_engine()
    raw_alunos.to_sql(name="alunos_curso", con=con, if_exists='replace')



