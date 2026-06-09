import io
import zipfile
import dagster as dg
import pandas as pd
from sqlalchemy.types import UnicodeText
from xml.etree import ElementTree as ET
from .resources import CorssrefApiResource, OrcidApiResource, TesesUspResource
from ..resources import SqlAlchemyResource, IcebergResource


@dg.asset(kinds={"pandas"})
def raw_lattes_data(
    context: dg.AssetExecutionContext, replicado_con: SqlAlchemyResource
) -> pd.DataFrame:
    query = """
       SELECT
        l.codpes,
        l.nompes,
        l.nomabvset,
        l.codema,
        l.nomfnc,
        l.tipvin,
        l.tipvinext,
        dpx.imgarqxml
        FROM DIM_PESSOA_XMLUSP dpx
        INNER JOIN LOCALIZAPESSOA l ON l.codpes = dpx.codpes
        WHERE l.codundclg = 14 AND l.sitatl = 'A' AND l.tipvinext IN ('Aluno de Pós-Graduação', 'Docente', 'Pós-doutorando')
    """
    con = replicado_con.get_engine()
    raw_data = pd.read_sql(con=con, sql=query)
    return raw_data


@dg.asset(kinds={"pandas"})
def publications_sanitized_data(raw_lattes_data: pd.DataFrame) -> pd.DataFrame:
    sanitized_data = raw_lattes_data.copy()
    sanitized_data["nomabvset"] = sanitized_data["nomabvset"].str.strip()
    sanitized_data["tipvin"] = sanitized_data["tipvin"].str.strip()
    return sanitized_data


@dg.asset(kinds={"pandas"})
def publications_orcid_id(publications_sanitized_data: pd.DataFrame) -> pd.DataFrame:
    xml_image_list = publications_sanitized_data["imgarqxml"].to_list()
    orcid_list = []
    for byte_code in xml_image_list:
        with io.BytesIO(byte_code) as zip_buffer:
            with zipfile.ZipFile(zip_buffer) as zf:
                file_names = zf.namelist()
                xml_name = file_names[0]
                xml_file = zf.read(xml_name)
        with io.BytesIO(xml_file) as xml_buffer:
            tree = ET.parse(xml_buffer)
            root = tree.getroot()
            general_data = root.find("DADOS-GERAIS")
            orcid_code = general_data.get("ORCID-ID")
        orcid_list.append(orcid_code)
    return pd.DataFrame({"orcid_id": orcid_list})


@dg.asset(kinds={"pandas"})
def publications_doi(
    context: dg.AssetExecutionContext, publications_sanitized_data: pd.DataFrame
):
    xml_image_list = publications_sanitized_data["imgarqxml"].to_list()
    doi_list = []
    for byte_code in xml_image_list:
        with io.BytesIO(byte_code) as zip_buffer:
            with zipfile.ZipFile(zip_buffer) as zf:
                file_names = zf.namelist()
                xml_name = file_names[0]
                xml_file = zf.read(xml_name)
        with io.BytesIO(xml_file) as xml_buffer:
            tree = ET.parse(xml_buffer)
            root = tree.getroot()
            producao_bibliografica = root.find("PRODUCAO-BIBLIOGRAFICA")
            if producao_bibliografica is not None:
                artigos = producao_bibliografica.findall(".//DADOS-BASICOS-DO-ARTIGO")
                for artigo in artigos:
                    doi = artigo.get("DOI", "")
                    doi_list.append(doi)
    return pd.DataFrame({"doi": doi_list})


@dg.asset(kinds={"pandas"})
def publications_with_orcid_and_doi(
    publications_sanitized_data: pd.DataFrame,
    publications_orcid_id: pd.DataFrame,
    publications_doi: pd.DataFrame,
) -> pd.DataFrame:
    df = publications_sanitized_data.copy()
    df["orcid_id"] = publications_orcid_id["orcid_id"]
    df["doi"] = publications_doi["doi"]
    df.drop(columns=["imgarqxml"], inplace=True)
    return df


@dg.asset(kinds={"pandas", "idceberg"})
def publications_s3_data(
    context: dg.AssetExecutionContext,
    iceberg_resource: IcebergResource,
    publications_with_orcid_and_doi: pd.DataFrame,
):
    df = publications_with_orcid_and_doi.copy()
    iceberg_resource.save(
        df, namespace="publications", table_name="raw_publications", context=context
    )
    return df


# @dg.asset()
# def publications_articles(
#     context: dg.AssetExecutionContext,
#     crossref_api: CorssrefApiResource,
#     publications_with_orcid_and_doi: pd.DataFrame,
# ):
#     orcid_id_list = publications_with_orcid_and_doi["orcid_id"].to_list()
#     publication_data_list = []
#     for orcid in orcid_id_list:
#         publication_row = publications_with_orcid_and_doi[
#             publications_with_orcid_and_doi["orcid_id"] == orcid
#         ]
#         if not orcid or not (isinstance(orcid, str)):
#             context.log.warning("ORCID ID is missing for ")
#             continue
#         context.log.info(f"Extracting publication data for ORCID ID: {orcid}")
#         orcid_number = orcid.split("/")[-1]
#         crossref_data = crossref_api.get_publications_by_orcid(
#             context=context, orcid=orcid_number
#         )
#         publication_data = crossref_api.format_publications_data(crossref_data)
#         aditional_data = {
#             "orcid_id": orcid,
#             "nompes": publication_row["nompes"].iloc[0],
#             "nomabvset": publication_row["nomabvset"].iloc[0],
#             "tipvin": publication_row["tipvin"].iloc[0],
#             "tipvinext": publication_row["tipvinext"].iloc[0],
#             "codema": publication_row["codema"].iloc[0],
#         }
#         publication_data = crossref_api.set_additional_publication_data(
#             publication_data, **aditional_data
#         )
#         publication_data_list.extend(publication_data)
#     publications_articles_df = pd.DataFrame(publication_data_list)
#     return publications_articles_df


# @dg.asset(kinds={"pandas"})
# def publications_orcid_api_data(
#     context: dg.AssetExecutionContext,
#     orcid_api: OrcidApiResource,
#     publications_with_orcid_and_doi: pd.DataFrame,
# ):
#     orcid_id_list = publications_with_orcid_and_doi["orcid_id"].to_list()
#     orcid_data_list = []
#     for orcid_id in orcid_id_list:
#         if not orcid_id or not (isinstance(orcid_id, str)):
#             context.log.warning(f"ORCID ID {orcid_id} is missing for")
#             continue
#         context.log.info(f"Extracting publication data for ORCID ID: {orcid_id}")
#         orcid_number = orcid_id.split("/")[-1]
#         orcid_data = orcid_api.get_works_dois_by_orcid(
#             context=context, orcid=orcid_number
#         )
#         orcid_data_list.extend(orcid_data)
#     return pd.DataFrame(orcid_data_list)


# @dg.asset(kinds={"pandas"})
# def publications_teses_data(teses_resource: TesesUspResource):
#     teses_data = teses_resource.extract_teses(page=1)
#     dict_list = [tese.to_dict() for tese in teses_data]
#     return pd.DataFrame(dict_list)


# @dg.asset(kinds={"pandas", "mysql"})
# def publications_perssisted_data(
#     mysql_con: SqlAlchemyResource, publications_articles: pd.DataFrame
# ):
#     data_type = {
#         "abstract": UnicodeText(collation="utf8mb4_unicode_ci"),
#         "title": UnicodeText(collation="utf8mb4_unicode_ci"),
#     }
#     con = mysql_con.get_engine()
#     publications_articles.to_sql(
#         name="publications", con=con, if_exists="replace", index=False, dtype=data_type
#     )
