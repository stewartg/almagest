import os

from opensearchpy import OpenSearch


class Singleton(type):
    def __init__(cls, *args, **kwargs):
        cls.__instance = None
        super().__init__(*args, **kwargs)

    def __call__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super().__call__(*args, **kwargs)
        return cls.__instance


class ClientHelper(metaclass=Singleton):
    def __init__(self):
        """Sets up the singleton opensearch client."""
        self.client = None
        self.user = ""
        self.pw = ""
        self._host = ""
        try:
            self._host = os.environ["OPENSEARCH_HOST"]
            self.user = os.environ["OPENSEARCH_USER"]
            self.pw = os.environ["OPENSEARCH_PW"]
            self.client_cert = os.getenv("OPENSEARCH_CERT_PATH")
            self.client_key = os.getenv("OPENSEARCH_KEY_PATH")
        except KeyError as err:
            os_env_vars = "OPENSEARCH_HOST, OPENSEARCH_USER, OPENSEARCH_PW"
            raise ValueError(f"The following environment variables must be set: {os_env_vars}") from err

    @classmethod
    def get_client(cls, verify_certs: bool = False):
        """Creates the opensearch client using environment variables.

        :raises ValueError: if the client can't connect
        :return: the opensearch client.
        """
        obj = cls()
        auth = (obj.user, obj.pw)
        client = OpenSearch(
            hosts=[obj.host],
            http_auth=auth,
            scheme="https",
            port=443,
            client_cert=obj.client_cert,
            client_key=obj.client_key,
            use_ssl=True,
            verify_certs=verify_certs,
            ssl_show_warn=True,
        )
        if not client.ping():
            raise ValueError("Could not connect to opensearch at %s", obj.host)
        return client

    @property
    def host(self):
        """Getter for OpenSearch hosts.

        :return: List of OpenSearch hosts.
        """
        return self._host
