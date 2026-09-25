from datetime import datetime, timedelta, date
from time import sleep
import dagster as dg
import requests
import random

LISTAR_LOG_THRESHOLD = 67
REQUEST_DELAY_SECONDS = 10

class HorusResource(dg.ConfigurableResource):
    base_url: str
    proxy_server: str
    username: str
    password: str

    def get_token(self) -> str:
        headers = {
            "accept": "*/*",
            "usr": self.username,
            "pwd": self.password
        }
        proxies = {
            "http": self.proxy_server,
            "https": self.proxy_server
        }
        endpoint = f"{self.base_url}/obterToken"
        response = requests.post(endpoint, headers=headers, proxies=proxies)
        response.raise_for_status()
        return response.json()["token"]

    def calc_date_interval(self, initial_date: datetime) -> int:
        """
        Calcula a quantidade de dias entre uma data inicial e o dia de hoje (inclusive).

        :param ano_inicio: Ano da data inicial (ex: 2026)
        :param mes_inicio: Mês da data inicial (ex: 5)
        :param dia_inicio: Dia da data inicial (ex: 12)
        :return: Número de dias entre a data inicial e hoje, incluindo o dia inicial
        """
        today = date.today()
        return (today - initial_date).days + 1

    def get_date_list(self, initial_date: datetime, end_date: datetime = None) -> list[datetime.date]:
        """
        Retorna uma lista de datas utilizadas para iterar sobre as requisições
        dos logs.
        """
        if end_date is None:
            end_date = datetime.today().date()

        date_list = []
        date_item = initial_date

        while date_item <= end_date:
            date_list.append(date_item)
            date_item += timedelta(days=1)
        return date_list

    def _request_log(self, token: str, date: datetime, endpoint: str, context: dg.AssetExecutionContext) -> list[dict]:
        date_to_str = date.strftime(format="%Y-%m-%d")
        headers = {
            "accept": "*/*",
            "authorization": f"Bearer {token}",
            "data": date_to_str
        }
        endpoint = f"{self.base_url}/{endpoint}"
        try:
            response = requests.post(endpoint, headers=headers)
            response.raise_for_status()
            return response.json()[0]
        except requests.exceptions.HTTPError as e:
            context.log.error(e)

    @staticmethod
    def _normalize_dict_keys(dict_list: list[dict], original_key: str) -> list[dict]:
        for item in dict_list:
            item["code_log"] = item.pop(original_key)
        return dict_list

    @staticmethod
    def _set_endpoint(logs: list[dict], endpoint_name: str) -> list[dict]:
        for item in logs:
            item["endpoint"] = endpoint_name
        return logs

    def get_missing_dates_list(self, start_date: datetime):
        threshould_date = datetime.now() - timedelta(days=90)
        if start_date > threshould_date.date():
            return self.get_date_list(initial_date=start_date)
        return self.get_date_list(initial_date=start_date, end_date=threshould_date.date())

    def listar_log_historico(self, token: str, date: datetime, context: dg.AssetExecutionContext) -> list[dict]:
        context.log.info("Requesting Historico")
        logs = self._request_log(token, date, "listarLogHistorico", context)
        logs = self._normalize_dict_keys(logs, original_key="codlogacessohistorico")
        return self._set_endpoint(logs, endpoint_name="historico")

    def listar_log(self, token: str, date: datetime, context: dg.AssetExecutionContext) -> list[dict]:
        context.log.info("Requesting Log")
        logs = self._request_log(token, date, "listarLog", context)
        logs = self._normalize_dict_keys(logs, original_key="codLogAcesso")
        return self._set_endpoint(logs, endpoint_name="logs")

    def extract_logs(self, date_interval: int, limit: int):
        if date_interval < limit:
            return self.listar_log
        return self.listar_log_historico   

    def get_a_break(self):
        start = 1
        limit = REQUEST_DELAY_SECONDS
        sleep_duration = random.randint(start, limit)
        sleep(sleep_duration)

    def iter_logs_since(self, start_date: datetime, context: dg.AssetExecutionContext):
        context.log.info(f"Starting log iteration from {start_date}")
        token = self.get_token()
        date_list = self.get_missing_dates_list(start_date=start_date)
        logs_list = []
        try:
            for date_item in date_list:
                context.log.info(f"Processing date {date_item}")
                date_interval = self.calc_date_interval(initial_date=date_item)
                logs = self.extract_logs(date_interval=date_interval, limit=LISTAR_LOG_THRESHOLD)(token=token, date=date_item, context=context)
                logs_list.extend(logs)
                context.log.info(f"Fetched {len(logs)} logs for {date_item}")
                self.get_a_break()
            return logs_list
        except Exception:
            return logs_list
