import dagster as dg
import requests
from pandas import DataFrame
from sqlalchemy import create_engine


class SqlAlchemyResource(dg.ConfigurableResource):
    connection_string: str
    
    def get_engine(self):
        engine = create_engine(self.connection_string)
        return engine

        

