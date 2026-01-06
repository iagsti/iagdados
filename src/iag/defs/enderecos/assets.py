import pandas as pd
import dagster as dg
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from .resources import SqlAlchemyResource


@dg.asset(kinds={"pandas"})
def raw_enderecos_dataframe(sqlserver_resource: SqlAlchemyResource) -> pd.DataFrame:
    engine = sqlserver_resource.get_engine()
    query = """
        SELECT
        DISTINCT
        tipo.nomtiplgr tipo,
        endereco.epflgr rua,
        endereco.numlgr numero,
        localidade.cidloc cidade,
        localidade.sglest estado,
        pais.nompas pais
        FROM ENDPESSOA endereco 
        INNER JOIN LOCALIDADE localidade ON localidade.codloc = endereco.codloc
        INNER JOIN TIPOLOGRADOURO tipo ON tipo.codtiplgr = endereco.codtiplgr
        INNER JOIN PAIS pais ON pais.codpas = localidade.codpas
        INNER JOIN LOCALIZAPESSOA pessoa ON pessoa.codpes = endereco.codpes
        WHERE pessoa.codundclg = 14 AND pessoa.tipvinext NOT IN ('Servidor Designado')
    """
    df = pd.read_sql(query, con=engine)
    return df


@dg.asset(kinds={"pandas"})
def enderecos_table(mysql_resource: SqlAlchemyResource, raw_enderecos_dataframe: pd.DataFrame) -> pd.DataFrame:
    engine = mysql_resource.get_engine()
    raw_enderecos_dataframe.to_sql(name='enderecos', con=engine, if_exists='replace', index=False)
    return raw_enderecos_dataframe


@dg.asset(kinds={"pandas"})
def enderecos_geocoding_dataframe(raw_enderecos_dataframe: pd.DataFrame) -> pd.DataFrame:
    df = raw_enderecos_dataframe.copy()
    geolocator = Nominatim(user_agent="iag")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    df['endereco_completo'] = df['rua'] + ", " + df['numero'] + ", " + df['cidade'] + " - " + df['estado'] + ", " + df['pais']
    df['location'] = df['endereco_completo'].apply(geocode)
    df['latitude'] = df['location'].apply(lambda loc: loc.latitude if loc else None)
    df['longitude'] = df['location'].apply(lambda loc: loc.longitude if loc else None)
    return df


@dg.asset(kinds={"pandas"})
def enderecos_geocoded_table(mysql_resource: SqlAlchemyResource, enderecos_geocoding_dataframe: pd.DataFrame) -> pd.DataFrame:
    engine = mysql_resource.get_engine()
    enderecos_geocoding_dataframe.to_sql(name='enderecos_geocoded', con=engine, if_exists='replace', index=False)
    return enderecos_geocoding_dataframe
