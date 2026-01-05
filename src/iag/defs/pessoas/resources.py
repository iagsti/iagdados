import dagster as dg
from sqlalchemy import create_engine


class SqlAlchemyResource(dg.ConfigurableResource):
    connection_string: str
    
    def get_engine(self):
        engine = create_engine(self.connection_string)
        return engine