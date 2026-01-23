import dagster as dg

import requests
import pandas as pd
from spellchecker import SpellChecker
from pymongo import MongoClient
from time import sleep
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Float, func
from sqlalchemy.orm import declarative_base
from requests_tor import RequestsTor


class DataPathResource(dg.ConfigurableResource):
    data_path: str

    def get_data_path(self):
        return self.data_path


class CatalogGroupsResource(dg.ConfigurableResource):
    def get_selected_groups(self):
        selected_groups = [
            35,
            40,
            41,
            47,
            49,
            51,
            52,
            53,
            56,
            58,
            60,
            61,
            63,
            70,
            71,
            74,
            75,
            80,
            85
        ]
        return selected_groups


class ComprasGovAPIResource(dg.ConfigurableResource):
    base_url: str
    stauts_item: str = "true"
    headers: dict = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
    }

    def __make_request(self, url: str, params: dict):
        try:
            response = requests.get(url, params=params, headers=self.headers)
            return response
        except:
            tor = RequestsTor(tor_ports=(9050,), tor_cport=9051)
            response = tor.get(url, params=params, headers=self.headers)
            return response

    def extract_data(
            self,
            context: dg.AssetExecutionContext,
            reference_list: list,
            resource_name: str,
            page_width: int
    ):
        data_list = []

        for code in reference_list:
            page = 1
            has_more_pages = True
            while has_more_pages and not context.run.is_failure_or_canceled:
                try:
                    response = getattr(self, resource_name)(page=page, page_width=page_width, code=code)
                    data_list += (response.json()["resultado"])
                    has_more_pages = (response.json()["paginasRestantes"] > 0)
                    log_text = f"Appending page {page}, item {code}"
                    context.log.info(log_text)
                except:
                    context.log.error(response.url)
                    context.log.error(f"Can't get page {page}, item {code}")
                sleep(1)
                page = page + 1
        df = pd.DataFrame(data_list)
        return df
        

    def get_items(self, page: int, page_width: int, code: int):
        parameters = {
            "pagina": page,
            "tamanhoPagina": page_width,
            "codigoGrupo": code,
            "statusitem": self.stauts_item
        }
        items_url = f"{self.base_url}/modulo-material/4_consultarItemMaterial"
        response = self.__make_request(url=items_url, params=parameters)
        return response

    def get_preco(self, page: int, page_width: int, code: int):
        parameters = {
            "pagina": page,
            "tamanhoPagina": page_width,
            "codigoItemCatalogo": code,
        }
        preco_url = f"{self.base_url}/modulo-pesquisa-preco/1_consultarMaterial"
        response = self.__make_request(url=preco_url, params=parameters)
        return response
            

class SqlAlchemyResource(dg.ConfigurableResource):
    connection_string: str
    
    def get_engine(self):
        engine = create_engine(self.connection_string)
        return engine
    

class ComprasgovTableResource(dg.ConfigurableResource):
    def create_comprasgov_itens_table(self, engine):
        Base = declarative_base()
        class ComprasGovItens(Base):
            __tablename__ = "comprasgov_itens"
            id = Column(Integer, primary_key=True, autoincrement=True)
            codigo_item = Column(Integer)
            codigo_grupo = Column(Integer)
            nome_grupo = Column(String(255))
            codigo_classe = Column(Integer)
            nome_classe = Column(String(2048))
            codigo_pdm = Column(Integer)
            nome_pdm = Column(String(2048))
            descricao_item = Column(String(2048))
            status_item = Column(Boolean)
            item_sustentavel = Column(Boolean)
            codigo_ncm = Column(String(128))
            descricriao_ncm = Column(String(2048))
            data_hora_atualizacao = Column(String(255))

        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

        return ComprasGovItens


class PCATableResource(dg.ConfigurableResource):
    def create_pca_itens_table(self, engine):
        Base = declarative_base()
        class PCAItens(Base):
            __tablename__ = 'core_item'
            id = Column(Integer, primary_key=True, autoincrement=True)
            data_criacao = Column(DateTime, default=func.now(), nullable=False)
            data_modificacao = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
            codigo_grupo = Column(Integer, nullable=False)
            nome_grupo = Column(String(256), nullable=False)
            codigo_classe = Column(Integer, nullable=False)
            nome_classe = Column(String(256), nullable=False)
            codigo_pdm = Column(Integer, nullable=False)
            nome_pdm = Column(String(256), nullable=False)
            codigo_item = Column(Integer, nullable=False)
            descricao_item = Column(String(2048), nullable=False)
            valor_estimado = Column(Float, nullable=True)

        Base.metadata.create_all(engine)

        return PCAItens


class MongoResource(dg.ConfigurableResource):
    mongo_uri: str

    def get_client(self):
        client = MongoClient(self.mongo_uri)
        return client


class SpellCheckerResource(dg.ConfigurableResource):
    def check_text(self, text: str):
        if not isinstance(text, str) or text is None:
            return text
        
        if len(text.strip()) == 0:
            return text
        
        spell = SpellChecker(language="pt")
        words = text.split()
        misspeled = spell.unknown(words)
        
        new_text = []
        
        for word in words:
            if word in misspeled:
                corrected = spell.correction(word)
                new_text.append(corrected if corrected else word)
            else:
                new_text.append(word)
        return " ".join(new_text)
    