import dagster as dg
import hashlib
from sqlalchemy import create_engine


class SqlAlchemyResource(dg.ConfigurableResource):
    connection_string: str

    def get_engine(self):
        engine = create_engine(self.connection_string)
        return engine


class ObfuscatorResource(dg.ConfigurableResource):
    def obfuscate(self, codpes: int | str, length: int = 10):
        """
        Obfusca um código (int ou string) de forma determinística usando SHA-256.

        :param code: O código a ser ofuscado (int ou str)
        :param length: Tamanho do hash de saída (padrão: 10 caracteres)
        :return: String com o código ofuscado
        """
        if not isinstance(codpes, (str, int)):
            raise TypeError("O código deve ser uma string ou um número inteiro.")
        code_str = str(codpes)
        hash_obj = hashlib.sha256(code_str.encode())
        hash_hex = hash_obj.hexdigest()
        return hash_hex[:length]
