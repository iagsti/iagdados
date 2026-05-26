from io import BytesIO
import zipfile
import dagster as dg
import xml.etree.ElementTree as ET
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, Text


class LattesExtractorResource(dg.ConfigurableResource):
    def _load_zip_file(self, binary_content):
        buffer_zip = BytesIO(binary_content)
        return buffer_zip

    def _extract_xml_content(self, zip_file, context: dg.AssetExecutionContext):
        with zipfile.ZipFile(zip_file) as zip_file:
            xml_file_name = [n for n in zip_file.namelist() if n.endswith(".xml")][0]
            context.log.info(f"Extracting XML file: {xml_file_name}")
            with zip_file.open(xml_file_name) as xml_file:
                return xml_file.read()

    def _extract_lattes_link_from_xml(
        self, xml_content, context: dg.AssetExecutionContext
    ):
        try:
            root = ET.fromstring(xml_content)
            identification_number = root.get("NUMERO-IDENTIFICADOR")
            if identification_number:
                context.log.info(f"Extracting identification: {identification_number}")
                return f"http://lattes.cnpq.br/{identification_number}"
            return ""
        except Exception as e:
            context.log.error(f"Error extracting Lattes link from XML: {e}")
            return None

    def extract_xml_content(self, binary_content, context: dg.AssetExecutionContext):
        zip_file = self._load_zip_file(binary_content)
        xml_content = self._extract_xml_content(zip_file, context)
        lattes_url = self._extract_lattes_link_from_xml(xml_content, context)
        return lattes_url


class LattesLinkTableResource(dg.ConfigurableResource):
    def create_table(self, engine):
        Base = declarative_base()

        class Publications(Base):
            __tablename__ = "lattes_links"
            id = Column(Integer, primary_key=True, autoincrement=True)
            codpes = Column(Integer, nullable=False)
            lattes_link = Column(Text, nullable=False)

        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

        return Publications
