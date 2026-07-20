from datetime import datetime, timedelta, date
import dagster as dg
import requests


class HorusResource(dg.ConfigurableResource):
    base_url: str
    username: str
    password: str

    def get_token(self):
        headers = {
            "accept": "*/*",
            "usr": self.username,
            "pwd": self.password
        }
        endpoint = f"{self.base_url}/obterToken"
        response = requests.post(endpoint, headers=headers)
        response.raise_for_status()
        response.raise_for_status()
        return response.json()["token"]
    
    def get_my_ip(self):
        response = requests.get("https://ifconfig.me")
        return response
    
    def calc_date_interval(self, ano_inicio, mes_inicio, dia_inicio):
        """
        Calcula a quantidade de dias entre uma data inicial e o dia de hoje.

        :param ano_inicio: Ano da data inicial (ex: 2026)
        :param mes_inicio: Mês da data inicial (ex: 5)
        :param dia_inicio: Dia da data inicial (ex: 12)
        :param inclusive: Se True, inclui o dia de início na contagem (+1 dia)
        :return: Inteiro representando o número de dias
        """

        data_inicial = date(ano_inicio, mes_inicio, dia_inicio)
        hoje = date.today()
        diferenca = hoje - data_inicial
        total_dias = diferenca.days
        total_dias += 1

        return total_dias

    def get_date_list(self, initial_date: str, end_date: str = ""):
        date_format = "%Y-%m-%d"
        initial = datetime.strptime(initial_date, "%Y/%m/%d").date()
        end = datetime.today().date() if not end_date else datetime.strptime(end_date, date_format).date()
        date_list = []
        current_date = initial

        while current_date <= end:
            date_list.append(current_date.strftime(date_format))
            current_date += timedelta(days=1)
        return date_list

    def _request_log(self, token: str, data: str, endpoint: str, context: dg.AssetExecutionContext):
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
            ip = self.get_my_ip()
            context.log.error(ip.text)
            return []
        
    def _normalize_dict_keys(self, dict_list: list[dict], original_key: str):
        for item in dict_list:
            new_dict = {"code_log": item.pop(original_key)}
            item.update(**new_dict)
        return dict_list
    
    def _set_endpoint(self, logs: list[dict], endpoint_name: str):
        for item in logs:
            item["endpoint"] = endpoint_name
        return logs

    def listar_log_historico(self, token: str, data: str, context: dg.AssetExecutionContext) -> list:
        context.log.info("Requesting Historico")
        logs = self._request_log(token, data, "listarLogHistorico", context)
        logs = self._normalize_dict_keys(dict_list=logs, original_key="codlogacessohistorico")
        logs = self._set_endpoint(logs=logs, endpoint_name="historico")
        return logs
    
    def listar_log(self, token: str, data: str, context: dg.AssetExecutionContext) -> list:
        context.log.info("Requesting Log")
        logs = self._request_log(token, data, "listarLog", context)
        logs = self._normalize_dict_keys(dict_list=logs, original_key="codLogAcesso")
        logs = self._set_endpoint(logs=logs, endpoint_name="logs")
        return logs

    def update_logs(self, date_interval: int, limit: int, token: str, data: str, context: dg.AssetExecutionContext):
        context.log.info(f"Interval: {date_interval}, limit: {limit}")
        if date_interval < limit:
            return self.listar_log(token, data, context)
        return self.listar_log_historico(token, data, context)

