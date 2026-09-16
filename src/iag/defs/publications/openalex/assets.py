import dagster as dg
import pandas as pd
from itertools import batched
from ...resources import SqlAlchemyResource, IcebergResource
from .resources import OpenAlexResource


@dg.asset(kinds={"sqlserver", "pandas"})
def openalex_docentes(openalex_db_source: SqlAlchemyResource):
    query = """
        SELECT l.nompes
        FROM LOCALIZAPESSOA l
        WHERE l.tipvinext = 'Docente' AND l.codundclg = 14
        ORDER BY l.nompes
    """
    con = openalex_db_source.get_engine()
    df = pd.read_sql(query, con=con)
    return df


@dg.asset()
def openalex_docentes_cleaned_nompes(openalex_docentes: pd.DataFrame):
    df = openalex_docentes.copy()
    df["nompes"] = df["nompes"].str.strip()
    return df


@dg.asset()
def openalex_articles(
    context: dg.AssetExecutionContext,
    openalex_resource: OpenAlexResource,
    openalex_docentes_cleaned_nompes: pd.DataFrame
):
    author_list_name = openalex_docentes_cleaned_nompes["nompes"].to_list()
    articles_collection = []
    for author in author_list_name:
        msg = f"Extraindo artigos de {author}"
        context.log.info(msg=msg)
        try:
            articles = openalex_resource.extract_articles(author_name=author)
        except Exception as e:
            context.log.error(f"Falha ao extrair artigos de {author}: {e}")
            continue
        context.log.info(articles)
        articles_collection.extend(articles)
    articles_dataframe = pd.DataFrame(articles_collection)
    return articles_dataframe


@dg.asset()
def openalex_persisted_articles(context: dg.AssetExecutionContext, iceberg_resource: IcebergResource, openalex_articles: pd.DataFrame):
    chunk_len = 100
    articles_list = openalex_articles.to_dict(orient="records")
    article_chunks = list(batched(articles_list, chunk_len))
    for articles in article_chunks:
        iceberg_resource.append(
            list(articles),
            context=context,
            namespace="publications",
            table_name="articles",
            from_type="from_pylist",
            **{},
        )
