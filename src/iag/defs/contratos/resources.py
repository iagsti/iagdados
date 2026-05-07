import dagster as dg
import requests
from time import sleep


class ContratosAPIResource(dg.ConfigurableResource):
    base_url: str
    
    def make_request(self, context: dg.AssetExecutionContext, params: dict = None):
        headers = {"Accept": "application/json", "Agent": "Mozilla/5.0"}
        response = requests.get(self.base_url, params=params, headers=headers)
        response.raise_for_status()
        context.log.info(response.url)
        return response.json()

    def set_params(self, start, page_length):
        lq = "codunddsp:14"
        params = {
            "name": "compra",
            "length": page_length,
            "start": start,
            "lq": lq,
            "q": ""
        }
        return params

    def get_contratos(self, context: dg.AssetExecutionContext):
        has_more_data = True
        raw_data_list = []
        start = 0
        page_length = 10

        while has_more_data:
            params = self.set_params(start=start, page_length=page_length)
            data = self.make_request(params=params, context=context)
            current_page = data.get("current_page")
            
            total_pages = data.get("total_pages")
            has_more_data = current_page <= total_pages
            start += page_length
            raw_data_list.extend(data.get("items"))
            message = f"Page {current_page} extracted"
            context.log.info(message)
            sleep(1)
        return raw_data_list