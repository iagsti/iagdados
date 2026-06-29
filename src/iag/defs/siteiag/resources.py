import base64
import time
import requests
import pandas as pd
import dagster as dg # Import padrão do Dagster em 2026

class WordPressIngestionResource(dg.ConfigurableResource):
    api_url: str
    username: str
    password: str

    def _get_headers(self) -> dict:
        return {
            "Content-Type": "application/json",
        }

    def ingerir_linha(self, row: pd.Series, context: dg.AssetExecutionContext) -> bool:
        """Processa e envia uma linha individual do DataFrame para o WordPress."""
        headers = self._get_headers()
        auth = (self.username, self.password)
        meta_fields = {
            "cargo": str(row.get("nomfnc", "")) if pd.notna(row.get("nomfnc")) else "",
            "funcao": str(row.get("tipfnc", "")) if pd.notna(row.get("tipfnc")) else "",
            "email": str(row.get("codema", "")) if pd.notna(row.get("codema")) else "",
            "telefone": str(row.get("telefone", "")) if pd.notna(row.get("telefone")) else "",
            "setor": str(row.get("nomset", "")) if pd.notna(row.get("nomset")) else "",
            "sala": str(row.get("sala", "")) if pd.notna(row.get("sala")) else "",
            "lattes": str(row.get("lattes_link", "")) if pd.notna(row.get("lattes_link")) else "",
        }

        nome_pessoa = str(row.get("nompes", "Sem Nome")) if pd.notna(row.get("nompes")) else "Sem Nome"
        payload = {
            "title": nome_pessoa,
            "status": "publish",
            "acf": meta_fields
        }

        try:
            response = requests.post(self.api_url, auth=auth, json=payload, headers=headers, verify=False)
            if response.status_code == 201:
                return True
            else:
                context.log.info(f"❌ Erro em {nome_pessoa}: HTTP {response.status_code} - {response.text} ")
                return False
        except Exception as e:
            print(f"💥 Falha de conexão ao enviar {nome_pessoa}: {e}")
            return False

    def deletar_todos(self, context: dg.AssetExecutionContext = None) -> int:
        """Remove todos os registros existentes no endpoint antes da ingestão."""
        logger = context.log if context else dg.get_dagster_logger()
        auth = (self.username, self.password)
        headers = self._get_headers()
        deletados = 0
        page = 1

        while True:
            response = requests.get(
                self.api_url,
                auth=auth,
                headers=headers,
                params={"per_page": 100, "page": page},
                verify=False,
            )
            if response.status_code != 200 or not response.json():
                break

            registros = response.json()
            for registro in registros:
                rid = registro.get("id") if isinstance(registro, dict) else registro
                del_response = requests.delete(
                    f"{self.api_url}/{rid}",
                    auth=auth,
                    headers=headers,
                    params={"force": True},
                    verify=False,
                )
                if del_response.status_code == 200:
                    deletados += 1
                else:
                    logger.info(f"Erro ao deletar id={rid}: HTTP {del_response.status_code}")

            if len(registros) < 100:
                break
            page += 1

        logger.info(f"Remoção concluída: {deletados} registros deletados.")
        return deletados

    def ingerir_dataframe(self, df: pd.DataFrame, context: dg.AssetExecutionContext = None) -> dict:
        """
        Percorre o DataFrame realizando a ingestão em lote (bulk).
        Integra com o context do Dagster se fornecido para logs nativos.
        """
        logger = context.log if context else dg.get_dagster_logger()
        logger.info(f"Iniciando ingestão de {len(df)} registros via WordPress Resource...")

        sucessos = 0
        falhas = 0

        for index, row in df.iterrows():
            resultado = self.ingerir_linha(row, context)
            if resultado:
                sucessos += 1
            else:
                falhas += 1
            
            # Controle de taxa (Rate limit)
            time.sleep(0.2)

        logger.info(f"Fim da ingestão. Sucessos: {sucessos} | Falhas: {falhas}")
        return {"sucessos": sucessos, "falhas": falhas}