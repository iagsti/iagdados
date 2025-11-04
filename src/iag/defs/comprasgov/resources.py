import requests
import dagster as dg


class ComprasGovAPIResource(dg.ConfigurableResource):
    base_url: str
    stauts_item: bool = True

    def get_items(self, page: int, page_width: int, group_code: int):
        parameters = {
            "pagina": page,
            "tamanhoPagina": page_width,
            "codigoGrupo": group_code,
            "statusitem": self.stauts_item
        }
        response = requests.get(self.base_url, params=parameters)
        return response.json()
