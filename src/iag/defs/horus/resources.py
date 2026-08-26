from datetime import datetime, timedelta, date
from time import sleep
import dagster as dg
import requests

RECENT_LOG_ENDPOINT_LIMIT = 67
REQUEST_DELAY_SECONDS = 2


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

    def get_my_ip(self) -> requests.Response:
        return requests.get("https://ifconfig.me")

    def calc_date_interval(self, ano_inicio: int, mes_inicio: int, dia_inicio: int) -> int:
        """
        Calcula a quantidade de dias entre uma data inicial e o dia de hoje (inclusive).

        :param ano_inicio: Ano da data inicial (ex: 2026)
        :param mes_inicio: Mês da data inicial (ex: 5)
        :param dia_inicio: Dia da data inicial (ex: 12)
        :return: Número de dias entre a data inicial e hoje, incluindo o dia inicial
        """
        data_inicial = date(ano_inicio, mes_inicio, dia_inicio)
        hoje = date.today()
        return (hoje - data_inicial).days + 1

    def get_date_list(self, initial_date: str, end_date: str = "") -> list[str]:
        date_format = "%Y-%m-%d"
        initial = datetime.strptime(initial_date, "%Y/%m/%d").date()
        end = datetime.today().date() if not end_date else datetime.strptime(end_date, date_format).date()
        date_list = []
        current_date = initial

        while current_date <= end:
            date_list.append(current_date.strftime(date_format))
            current_date += timedelta(days=1)
        return date_list

    def _request_log(self, token: str, data: str, endpoint: str, context: dg.AssetExecutionContext) -> list[dict]:
        headers = {
            "accept": "*/*",
            "authorization": f"Bearer {token}",
            "data": data
        }
        endpoint = f"{self.base_url}/{endpoint}"
        try:
            response = requests.post(endpoint, headers=headers)
            response.raise_for_status()
            return response.json()[0]
        except Exception as e:
            context.log.error(e)
            context.log.error(self.get_my_ip().text)
            return []

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

    def listar_log_historico(self, token: str, data: str, context: dg.AssetExecutionContext) -> list[dict]:
        context.log.info("Requesting Historico")
        logs = self._request_log(token, data, "listarLogHistorico", context)
        logs = self._normalize_dict_keys(logs, original_key="codlogacessohistorico")
        return self._set_endpoint(logs, endpoint_name="historico")

    def listar_log(self, token: str, data: str, context: dg.AssetExecutionContext) -> list[dict]:
        context.log.info("Requesting Log")
        logs = self._request_log(token, data, "listarLog", context)
        logs = self._normalize_dict_keys(logs, original_key="codLogAcesso")
        return self._set_endpoint(logs, endpoint_name="logs")

    def update_logs(
        self, date_interval: int, limit: int, token: str, data: str, context: dg.AssetExecutionContext
    ) -> list[dict]:
        context.log.info(f"Interval: {date_interval}, limit: {limit}")
        if date_interval < limit:
            return self.listar_log(token, data, context)
        return self.listar_log_historico(token, data, context)

    def iter_logs_since(self, start_date: str, context: dg.AssetExecutionContext):
        """
        Itera dia a dia os logs de acesso desde `start_date` (formato "%Y/%m/%d") até hoje,
        produzindo (data, logs) a cada dia processado — em vez de acumular tudo em memória.
        """
        date_list = self.get_date_list(initial_date=start_date)
        token = self.get_token()

        for date_item in date_list:
            context.log.info(f"Processing date {date_item}")
            # Recalculado por dia: a API usa um endpoint para datas recentes e outro
            # para datas antigas, então o intervalo precisa refletir cada `date_item`,
            # não apenas o `start_date` original.
            ano, mes, dia = (int(part) for part in date_item.split("-"))
            date_interval = self.calc_date_interval(ano_inicio=ano, mes_inicio=mes, dia_inicio=dia)
            logs = self.update_logs(
                date_interval=date_interval,
                limit=RECENT_LOG_ENDPOINT_LIMIT,
                token=token,
                data=date_item,
                context=context,
            )
            context.log.info(f"Fetched {len(logs)} logs for {date_item}")
            yield date_item, logs
            sleep(REQUEST_DELAY_SECONDS)

