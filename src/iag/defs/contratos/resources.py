import dagster as dg
import requests
import random
from time import sleep


class ContratosAPIResource(dg.ConfigurableResource):
    base_url: str
    
    def wait(self):
        seconds = random.randint(3, 10)
        sleep(seconds)
    
    def make_request(self, context: dg.AssetExecutionContext, params: dict = None):
        agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0"
        headers = {"Accept": "application/json", "Agent": agent}
        try:
            response = requests.get(self.base_url, params=params, headers=headers)
            response.raise_for_status()
            context.log.info(response.url)
            self.wait()
            return response.json()
        except Exception as e:
            context.log.error(f"Erro ao acessar url: {response.url} / {e}")
            return None

    def set_params(self, page_length, name, **kwargs):
        params = {
            "name": name,
            "length": page_length,
        }
        params.update(**kwargs)
        return params

    def get_contratos(self, context: dg.AssetExecutionContext):
        name = "compra"
        has_more_data = True
        raw_data_list = []
        page_length = 10
        start = 0
        while has_more_data:
            kwargs = {"start": start, "lq" :"codunddsp:14", "q": ""}
            params = self.set_params(page_length=page_length, name=name, **kwargs)
            data = self.make_request(params=params, context=context)
            if data:
                current_page = data.get("current_page")
                total_pages = data.get("total_pages")
                has_more_data = current_page <= total_pages
                start += page_length
                raw_data_list.extend(data.get("items"))
                message = f"Page {current_page} extracted"
                context.log.info(message)
        return raw_data_list
    
    def get_item(self, context: dg.AssetExecutionContext, codpcddsp: int):
        name = "compraitem"
        page_length = 1000
        lq = f"codpcddsp:{str(codpcddsp)}"
        kwargs = {"sortBy": "numord", "desc": "N", "lq": lq}
        params = self.set_params(page_length=page_length, name=name, **kwargs)
        data = self.make_request(params=params, context=context)
        return data
    
    def get_files(self, codpcddsp: int, context: dg.AssetExecutionContext):
        name = "compraarq"
        page_length = 1000
        lq = f"codpcddsp:{str(codpcddsp)}"
        kwargs = {"sortBy": "numseq", "desc": "N", "lq": lq}
        params = self.set_params(page_length=page_length, name=name, **kwargs)
        data = self.make_request(params=params, context=context)
        return data
