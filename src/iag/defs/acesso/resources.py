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


class AcessoResource(dg.ConfigurableResource):
    api_url: str
    api_user: str
    api_password: str

    def get_pessoaspos(self) -> pd.DataFrame:
        """
        Fetch data from the Acesso API and return it as a pandas DataFrame.
        """
        response = requests.get(
            f"{self.api_url}/pessoas-pos",
            auth=(self.api_user, self.api_password),
        )
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data)

    def upsert_pessoaspos(self, df: pd.DataFrame) -> None:
        """
        Upsert the provided DataFrame into the Acesso API.
        """
        url = f"{self.api_url}/pessoas-pos/"
        data = [
            {k: _json_safe(v) for k, v in record.items()}
            for record in df.to_dict(orient="records")
        ]
        auth = (self.api_user, self.api_password)
        response = requests.post(url, json=data, auth=auth)
        response.raise_for_status()

    def soft_delete(self, num_usp: str) -> None:
        """
        Soft delete the provided DataFrame in the Acesso API.
        """
        url = f"{self.api_url}/pessoas-pos/{num_usp}/"
        auth = (self.api_user, self.api_password)
        response = requests.delete(url, auth=auth)
        response.raise_for_status()