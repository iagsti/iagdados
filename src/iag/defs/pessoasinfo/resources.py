import requests
import dagster as dg
import pandas as pd


class LocalsApiResource(dg.ConfigurableResource):
    api_url: str

    def _make_api_request(self):
        response = requests.get(self.api_url)
        return response.json()

    def _get_additional_data_field(self, field, field_type="str"):
        try:
            field_value = field[0]["value"]
            if field_type == "int":
                return int(field_value)
            return field_value
        except IndexError:
            return ""

    def get_locals(self):
        data_list = self._make_api_request()
        selected_data = []
        for data in data_list:
            codpes = self._get_additional_data_field(
                data["field_tel_numero_usp"], field_type="int"
            )
            ramal = self._get_additional_data_field(data["field_tel_ramal"])
            bloco = self._get_additional_data_field(data["field_tel_bloco"])
            sala = self._get_additional_data_field(data["field_tel_sala"])
            selected_data.append(
                {"codpes": codpes, "ramal": ramal, "sala": f"{bloco}-{sala}"}
            )

        additional_data = pd.DataFrame(selected_data)
        return additional_data


class PessoasResource(dg.ConfigurableResource):
    def get_sites(self):
        return [
            {"codpes": 141126, "url": "http://astroweb.iag.usp.br/~carciofi"},
            {"codpes": 2087915, "url": "http://astroweb.iag.usp.br/~amancio"},
            {"codpes": 2083484, "url": "http://astroweb.iag.usp.br/~mario"},
            {"codpes": 69810, "url": "http://astroweb.iag.usp.br/~damineli"},
            {"codpes": 79669, "url": "http://astroweb.iag.usp.br/~barbuy"},
            {"codpes": 95000, "url": "http://astroweb.iag.usp.br/~oliveira"},
            {"codpes": 2598317, "url": "http://astroweb.iag.usp.br/~janot"},
            {"codpes": 52965, "url": "http://astroweb.iag.usp.br/~dalpino"},
            {"codpes": 1000180, "url": "http://astroweb.iag.usp.br/~gastao"},
            {"codpes": 25363, "url": "http://astroweb.iag.usp.br/~jacques"},
            {"codpes": 66286, "url": "http://astroweb.iag.usp.br/~foton"},
            {"codpes": 2521673, "url": "http://astroweb.iag.usp.br/~jorge"},
            {"codpes": 3076202, "url": "http://astroweb.iag.usp.br/~limajas"},
            {"codpes": 70187, "url": "http://astroweb.iag.usp.br/~laerte"},
            {"codpes": 1253683, "url": "http://astroweb.iag.usp.br/~pcoelho"},
            {"codpes": 5127380, "url": "http://astroweb.iag.usp.br/~gali"},
            {"codpes": 54219, "url": "http://astroweb.iag.usp.br/~roberto"},
            {"codpes": 8739721, "url": "http://astroweb.iag.usp.br/~nemmen"},
            {"codpes": 53625, "url": "http://astroweb.iag.usp.br/~sandra"},
            {"codpes": 86855, "url": "http://astroweb.iag.usp.br/~rossi"},
            {"codpes": 42776, "url": "http://astroweb.iag.usp.br/~sylvio"},
            {"codpes": 1762934, "url": "http://astroweb.iag.usp.br/~thais"},
            {"codpes": 89802, "url": "http://astroweb.iag.usp.br/~jatenco"},
            {"codpes": 1235074, "url": "http://astroweb.iag.usp.br/~marcos"},
            {"codpes": 3297859, "url": "https://astroweb.iag.usp.br/~rubens"},
        ]

    def set_gender(self, terms):
        scaped_words = ["Doutorado", "Mestrado", "Direto"]
        try:
            new_term = []
            for term in terms.split(" "):
                term_listed = list(term)

                if term_listed[-1] == "o" and term not in scaped_words:
                    term_listed[-1] = "a"

                if "".join(term_listed[-2:]) == "or":
                    term_listed[-1] = "ra"

                new_item = "".join(term_listed)
                new_term.append(new_item)
            return " ".join(new_term)
        except Exception:
            return terms
