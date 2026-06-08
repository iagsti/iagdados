import orcid
import random
import requests
import dagster as dg
from time import sleep
from bs4 import BeautifulSoup
from typing import NamedTuple
from datetime import datetime
from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.orm import declarative_base, Mapped, mapped_column


class CorssrefApiResource(dg.ConfigurableResource):
    base_url: str
    
    def wait(self):
        seconds = random.randint(3, 10)
        sleep(seconds)
    
    def set_params(self, orcid: str) -> dict:
        return {
            "filter": f"orcid:{orcid}",
            "rows": 1000
        }
    
    def make_request(self, context: dg.AssetExecutionContext ,params: dict):
        agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0"
        headers = {"Accept": "application/json", "Agent": agent}
        try:
            response = requests.get(self.base_url, params=params, headers=headers)
            response.raise_for_status()
            context.log.info(response.url)
            self.wait()
            return response.json()
        except Exception as e:
            context.log.error(f"Erro ao acessar {response.url} / {e}")
            return None
    
    def get_publications_by_orcid(self, context: dg.AssetExecutionContext, orcid: str):
        params = self.set_params(orcid=orcid)
        crossref_response = self.make_request(context=context, params=params)
        return crossref_response

    def format_authors_data(self, authors: list) -> list:
        authors_list = []
        for author in authors:
            name = author.get("given", "") + " " + author.get("family", "")
            authors_list.append(name.strip())
        authors_string = "; ".join(authors_list)
        return authors_string
    
    def format_link_data(self, links: list) -> list:
        return links[0].get("URL") if links else ""
 
    def format_publications_data(self, coressref_data: dict) -> list:
        if not coressref_data:
            return {}
        items = coressref_data.get("message", {}).get("items", [])
        if not items:
            return {}
        items_list = []
        for item in items:
            published_online_data = item.get("published-online", {}).get("date-parts", [])
            published_online = "-".join(published_online_data)
            formatted_publication_data = {
                "indexed": item.get("indexed", {}).get("date-time"),
                "publisher": item.get("publisher"),
                "issue": item.get("issue"),
                "content_domain": item.get("content-domain", {}).get("domain", []),
                "published_print": item.get("published-print", {}).get("date-parts", []),
                "abstract": item.get("abstract"),
                "type": item.get("type"),
                "created": item.get("created", {}).get("date-time"),
                "page": item.get("page"),
                "title": item.get("title", [])[0] if item.get("title") else None,
                "prefix": item.get("prefix"),
                "volume": item.get("volume"),
                "authors": self.format_authors_data(item.get("author", [])),
                "member": item.get("member"),
                "published_online": datetime.fromisoformat(published_online),
                "link": self.format_link_data(item.get("link", [])),
                "ISSN": item.get("ISSN", []),
                "DOI": item.get("DOI"),
            }
            items_list.append(formatted_publication_data)
        return items_list
    
    def set_additional_publication_data(self, publication_data_list: list, **aditional_data) -> list:
        for publication in publication_data_list:
            publication.update(**aditional_data)
        return publication_data_list


class OrcidApiResource(dg.ConfigurableResource):
    client_id: str
    client_secret: str
    sandbox: bool = True

    def _get_works(self, orcid_id: str, context: dg.AssetExecutionContext):
        api = orcid.PublicAPI(self.client_id, self.client_secret, sandbox=self.sandbox)
        token = api.get_search_token_from_orcid()
        try:
            record = api.read_record_public(orcid_id, 'works', token)
            return record
        except Exception as e:
            context.log.error(f"Erro ao acessar dados do ORCID ID {orcid_id}: {e}")
            return None
    
    def _get_doi_from_external_ids(self, external_ids_container):
        if not external_ids_container:
            return None
        ids_list = external_ids_container.get("external-id", [])
        for item in ids_list:
            if item.get("external-id-type") == "doi":
                return item.get("external-id-value")
        return None
    
    def _extract_dois_from_orcid_works(self, works_record):
        dois = []
        groups = works_record.get("group", [])
        for group in groups:
            external_ids = group.get("external-ids")
            doi = self._get_doi_from_external_ids(external_ids)
            if doi:
                dois.append(doi)
        return list(set(dois)) 

    def get_works_dois_by_orcid(self, orcid_id: str, context: dg.AssetExecutionContext):
        works_record = self._get_works(orcid_id=orcid_id, context=context)
        if not works_record:
            return []
        dois = self._extract_dois_from_orcid_works(works_record)
        return dois


class TesesUspResource(dg.ConfigurableResource):
    base_url: str = "https://teses.usp.br/"
    unidade: str
    number_of_pages: int = 1

    def _get_tese_class(self):
        class Tese(NamedTuple):
            title: str
            author: str
            program: str
            level: str
            year: int
            department: str
            url: str

            def get_doi(self):
                doi_id = self.url.split("/")[-2]
                doi = f"10.11606/D.14{self.year.strip()}/{doi_id.strip()}"
                return doi
            
            def to_dict(self):
                return {
                    "title": self.title,
                    "author": self.author,
                    "program": self.program,
                    "level": self.level,
                    "year": self.year,
                    "department": self.department,
                    "url": self.url,
                    "doi": self.get_doi()
                }

        return Tese
    
    def _set_params(self, page: int):
        params = {
            "lang": "pt-br",
            "operadores[]": "AND",
            "campos[]": "departamento",
            "termos[]": self.unidade,
            "termos_exatos[]": 0,
            "page_size": 100,
            "ordenar_campo": "ano",
            "ordenar_direcao": "ano",
            "page": page
        }
        return params

    def _get_headers(self):
        agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0"
        headers = {"User-Agent": agent}
        return headers
    
    def _make_request(self, page: int):
        params = self._set_params(page=page)
        headers = self._get_headers()
        try:
            response = requests.get(self.base_url, params=params, headers=headers)
            response.raise_for_status()
            return response.text
        except Exception as e:
            return None
        
    def _extract_number_of_pages(self, html: str):
        soup = BeautifulSoup(html, "html.parser")
        pagination_status = soup.find(class_="pagination-status")
        pagination_text = pagination_status.text.strip()
        number_of_pages = int(pagination_text.split()[-1])
        return number_of_pages

    def _extract_teses_data(self, html: str):
        soup = BeautifulSoup(html, "html.parser")
        tbody = soup.find("tbody")
        rows = tbody.find_all("tr") if tbody else []
        teses_list = []
        for row in rows:
            tds = row.find_all("td")
            if len(tds) < 6:
                continue
            title_cell = tds[0]
            title = title_cell.find("a").text.strip()
            url = title_cell.find("a")["href"].strip()
            author = tds[1].text.strip()
            program = tds[2].text.strip()
            department = tds[3].text.strip()
            level = tds[4].text.strip()
            year = tds[5].text.strip()
            
            tese_class = self._get_tese_class()
            tese = tese_class(title, author, program, department, level, year, url)
            teses_list.append(tese)
        return teses_list

    def extract_teses(self, page: int, context: dg.AssetExecutionContext):
        html = self._make_request(page=page)
        if not html:
            return []
        number_of_pages = self._extract_number_of_pages(html=html)
        pages_range = range(1, number_of_pages + 1)
        teses_data = []
        for page in enumerate(pages_range):
            context.log.info(f"Extracting teses data from page {page} of {self.number_of_pages}")
            html = self._make_request(page=page)
            teses_data.extend(self._extract_teses_data(html=html))
        return teses_data
    

class PublicationsTableResource(dg.ConfigurableResource):
    def create_publications_table(self, engine):
        Base = declarative_base()
        class Publications(Base):
            __tablename__ = "publications"
            id = Column(Integer, primary_key=True, autoincrement=True)
            indexed = Mapped[datetime]
            publisher = Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
            issue = Column(Integer, nullable=True)
            content_domain = Column(Text, nullable=True)
            published_print = Column(Text, nullable=True)
            abstract = Column(Text, nullable=True)
            type = Column(String, nullable=True)
            created = Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
            page = Column(Integer, nullable=True)
            title = Column(String(255), nullable=True)
            prefix = Column(String(255), nullable=True)
            volume = Column(Integer, nullable=True)
            authors = Column(Text, nullable=True)
            member = Column(Integer, nullable=True)
            published_online = Column(DateTime, nullable=True)
            link = Column(String(255), nullable=True)
            ISSN = Column(String(255), nullable=True)
            DOI = Column(String(255), nullable=True)
            nompes = Column(String(255), nullable=True)
            nomabvset = Column(String(255), nullable=True)
            tipvin = Column(String(255), nullable=True)
            tipvinext = Column(String(255), nullable=True)
            codema = Column(Integer, nullable=True)


        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

        return Publications