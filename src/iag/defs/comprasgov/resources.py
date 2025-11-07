import dagster as dg
import requests
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.orm import declarative_base
from requests_tor import RequestsTor


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

    def get_items(self, page: int, page_width: int, group_code: int):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
        }
        parameters = {
            "pagina": page,
            "tamanhoPagina": page_width,
            "codigoGrupo": group_code,
            "statusitem": self.stauts_item
        }

        try:
            response = requests.get(self.base_url, params=parameters, headers=headers)
            return response
        except:
            tor = RequestsTor(tor_ports=(9050,), tor_cport=9051)
            response = tor.get(self.base_url, params=parameters, headers=headers)
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
            codigo_item = Column(Integer, primary_key=True, autoincrement=False)
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



