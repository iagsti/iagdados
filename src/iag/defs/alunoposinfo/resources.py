import dagster as dg
import requests


class AcessoResource(dg.ConfigurableResource):
    api_url: str
    api_user: str
    api_password: str

    def get_auth(self):
        return (self.api_user, self.api_password)

    def insert_pessoas(self, payload: list[dict]):
        endpoint = self.api_url
        auth = self.get_auth()
        response = requests.post(endpoint, json=payload, auth=auth)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(f"{e} - Response body: {response.text}", response=response) from e
        return response

    def delete_pessoas(self):
        endpoint = self.api_url
        auth = self.get_auth()
        response = requests.delete(endpoint, auth=auth)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(f"{e} - Response body: {response.text}", response=response) from e
        return response
