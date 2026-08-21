import requests
import dagster as dg
import pandas as pd


def _json_safe(value):
    """Converte tipos do pandas (Timestamp, NaT) para algo serializável em JSON."""
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    return value


def _to_records(df: pd.DataFrame) -> list[dict]:
    """Converte um DataFrame em uma lista de registros prontos pra JSON."""
    return [
        {k: _json_safe(v) for k, v in record.items()}
        for record in df.to_dict(orient="records")
    ]


class AcessoResource(dg.ConfigurableResource):
    api_url: str
    api_user: str
    api_password: str

    @property
    def _auth(self) -> tuple[str, str]:
        return (self.api_user, self.api_password)

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        response = requests.request(method, f"{self.api_url}{path}", auth=self._auth, **kwargs)
        response.raise_for_status()
        return response

    def get_pessoaspos(self) -> pd.DataFrame:
        """
        Fetch data from the Acesso API and return it as a pandas DataFrame.
        """
        response = self._request("GET", "/pessoas-pos")
        return pd.DataFrame(response.json())

    def upsert_pessoaspos(self, df: pd.DataFrame) -> None:
        """
        Upsert the provided DataFrame into the Acesso API.
        """
        self._request("POST", "/pessoas-pos/", json=_to_records(df))

    def soft_delete(self, num_usp: str) -> None:
        """
        Soft delete the pessoa with the given num_usp in the Acesso API.
        """
        self._request("DELETE", f"/pessoas-pos/{num_usp}/")

    def upsert_pessoasinfo(self, df: pd.DataFrame) -> None:
        """
        Upsert the provided DataFrame into the Acesso API's cadastro geral
        (Pessoa) — not /pessoas/, que apesar do nome grava em PessoaPos
        (roster de Pós-Graduação) e não faz upsert.
        """
        self._request("POST", "/pessoas-gerais/", json=_to_records(df))