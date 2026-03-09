import os
from operator import itemgetter
from pathlib import Path
from time import monotonic
from urllib.parse import urlparse

import requests
from OpenSSL import crypto
from requests.adapters import HTTPAdapter


class Singleton(type):
    """Singleton implementation.

    Creates a new class if one doesn't exist else return the existing class.
    :param type: the class type.
    """

    def __init__(cls, *args, **kwargs):
        cls.__instance = None
        super().__init__(*args, **kwargs)

    def __call__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super().__call__(*args, **kwargs)
        return cls.__instance


class RequestsHeaderHelper(metaclass=Singleton):
    """Provides headers and dictionary entries for use with requests.

    It also contains the logic to retrieve the cognos token for querying
    cognos endpoints. It is a singleton to maintain consistent timeout
    settings for the cognos token.
    """

    def __init__(self):
        self.referer_data = {}
        token_create_path = os.getenv("SECURITY_CREATE_PATH", "/token/create")
        certs_location = os.getenv("CERT_LOCATION", f'{os.environ["HOME"]}/.mycerts')
        self.security_base_url = os.getenv("SECURITY_URL", "")
        self.ca_path = os.getenv("CA_PATH", None)
        if not self.ca_path:
            self.ca_path = False
        self.max_token_age_sec = int(os.getenv("MAX_TOKEN_AGE", f"{60 * 20}"))  # 20 minutes
        self.security_path = f"{self.security_base_url}{token_create_path}"
        self._token_timestamp_sec = 0
        self._security_token = ""
        self._api_key = ""
        self.certs_data = {
            "KEYFILE": f"{certs_location}/key.pem",
            "CERTFILE": f"{certs_location}/cert.pem",
            "CAFILE": f"{certs_location}/ic-ca-bundle.crt",
        }

    def get_header_properties(self) -> list:
        """Retrieves the header property names in this class.

        :return: the list of header property names.
        """
        return [k for k, v in vars(type(self)).items() if isinstance(v, property)]

    def get_headers_by_index(self, indices: tuple[int, ...]) -> dict:
        """Retrieves the header properties by index instead of by name.

        To use,
        first retrieve the list of header property names and call this method
        with the indices of the desired header properties.
        :param indices: a list of indices that correspond to the header properties
        :return: a dictionary that contains the corresponding headers.
        """
        return self.get_headers(list(itemgetter(*indices)(self.get_header_properties())))

    def get_headers(self, properties: list) -> dict:
        """Get the headers using the header names provided in the properties list.

        :param properties: the header property names from this class
        :type properties: list
        :return: a dictionary that contains the corresponding headers.
        """
        headers: dict[str, str] = {}
        for func_name in properties:
            headers.update(getattr(self, func_name, {}))
        return headers

    def get_stock_headers(self, endpoint_url: str = "") -> dict:
        """Retrieves a set of common headers.

        Thos headers include the user_agent, accept_encoding, accept_language, and referer.
        :param endpoint_url: the referer url
        :return: a dictionary that contains the indicated headers.
        """
        headers: dict[str, str] = {}
        props = self.get_header_properties()[5:8]
        for prop_name in props:
            headers.update(getattr(self, prop_name, {}))
        if endpoint_url:
            self.referer = endpoint_url
            headers.update(self.referer_data)
        return headers

    def get_cert_param(self, cert_param: str):
        """Retrieves a cert param.

        :param cert_param: one of ['subject', 'issuer']
        :return: the found cert parameter.
        """
        ret_val = ""
        cert_file = self.certs_data["CERTFILE"]
        with Path.open(cert_file) as fin:
            cert = crypto.load_certificate(crypto.FILETYPE_PEM, fin.read())
            if cert_param == "subject":
                val = cert.get_subject()
            elif cert_file == "issuer":
                val = cert.get_issuer()
            ret_val = "".join(f"/{name.decode():s}={val.decode():s}" for name, val in val.get_components())
        return ret_val

    @property
    def content_json(self) -> dict:
        """Gets the content_json header.

        :return: the content type header entry dictionary.
        """
        return {"Content-Type": "application/json"}

    @property
    def accept_json(self) -> dict:
        """Gets the accept_json header.

        :return: the accept header entry dictionary.
        """
        return {"Accept": "application/json"}

    @property
    def cognos_client_headers(self) -> dict:
        """Gets the cognos_client_headers header.

        :return: a dictionary containing the cognos npe cert's distinguished
        name and the issuer distinguished name.
        """
        return {
            "SSL_CLIENT_S_DN": self.get_cert_param("subject"),
            "SSL_CLIENT_ISSUER_DN": self.get_cert_param("issuer"),
        }

    @property
    def bearer_auth(self) -> dict:
        """Gets a security token.

        This method will call the security_token getter below that will then
        retrieve the token.
        :return: a dictionary containing the authorization header with the token
        as the bearer.
        """
        return {"Authorization": f"Bearer {self.security_token}"}

    def proxie_headers(self, http_proxy: str = "", https_proxy: str = "") -> dict:
        """Gets the proxie_headers.

        Allows overriding the environment defaults if alternate proxy values are provided.
        :param http_proxy: the http proxy url
        :param https_proxy: the https proxy url
        :return: the proxy header entry dictionary.
        """
        return {
            "http": http_proxy if http_proxy else os.getenv("http_proxy", ""),
            "https": https_proxy if https_proxy else os.getenv("https_proxy", ""),
        }

    @property
    def user_agent(self) -> dict:
        """Gets the user_agent header.

        :return: a dictionary containing the user agent header data.
        """
        return {"User-Agent": "Mozilla/5.0 (X11; Linux X86_64; rv:52.0) Gecko/20100101 Firefox/52.0"}

    @property
    def accept_encoding(self) -> dict:
        """Gets the accept_encoding header.

        :return: a dictionary containing the accept encoding header data.
        """
        return {"Accept-Encoding": "gzip, deflate, br"}

    @property
    def accept_language(self) -> dict:
        """Gets the accept_language header.

        :return: a dictionary containing the accept language header data.
        """
        return {"Accept-Language": "en-US, en; q=0.5"}

    @property
    def referer(self) -> dict:
        """Gets the referer header. The referer must be set first.

        :return: a dictionary containing the referer header data.
        """
        return self.referer_data

    @referer.setter
    def referer(self, endpoint_url):
        """Sets the referer url by url parsing the referer endpoint.

        :param endpoint_url: the referer url.
        """
        if endpoint_url:
            parsed_endpt = urlparse(endpoint_url)
            self.referer_data = {"Referer": f"{parsed_endpt.scheme}://{parsed_endpt.netloc}"}

    @property
    def certs(self):
        """Gets the cognos npe cert and key file.

        :return: a tuple containing these files.
        """
        return (self.certs_data["CERTFILE"], self.certs_data["KEYFILE"])

    @certs.setter
    def certs(self, cert_data):
        """Sets the certs data."""
        self.certs_data = cert_data

    @property
    def ca_bundle(self):
        """Gets the CA bundle.

        :return: the CA bundler file.
        """
        return self.certs_data["CAFILE"]

    @property
    def security_token(self) -> str:
        """Refreshes and gets the token.

        :return: the token string.
        """
        self._refresh_security_token()
        return self._security_token

    def _refresh_security_token(self):
        """Retrieve a security token if it doesn't exist or has expired.

        First try to retrieve the token using the cert headers and if that fails attempt
        to use the cert itself.
        """
        now = monotonic()
        if not self._security_token or (now - self._token_timestamp_sec > self.max_token_age_sec):
            tok_resp = None
            with requests.Session() as session:
                adapter = HTTPAdapter(max_retries=int(os.getenv("TOKEN_RETRIES", "3")))
                session.mount("https://", adapter)
                session.mount("http://", adapter)
                try:
                    tok_resp = session.get(
                        self.security_path, headers=self.cognos_client_headers, verify=False, timeout=30
                    )
                    tok_resp.raise_for_status()
                except Exception:
                    tok_resp = session.get(self.security_path, cert=self.certs, verify=False, timeout=30)
                    tok_resp.raise_for_status()
                if tok_resp:
                    self._security_token = tok_resp.text
                    self._token_timestamp_sec = now


if __name__ == "__main__":
    rh = RequestsHeaderHelper()
    # stk_head = rh.get_stock_headers(
    #     'https://gstore.unm.edu/apps/rgisarchive/datasets/7bbe8af5-029b-4adf-b06c-134f0dd57226/'
    #     'services/ogc/wms'
    # )
    # tmp = [n for n in dir(rh) if isinstance(getattr(rh.__class__, n), property)]
    # tmp = [k for k, v in rh.__dict__.items() if isinstance(v, property)]
    abc = rh.get_headers(["content_json", "accept_json"])
    hdr = {"Authorization": "Basic hello"} | abc
    tmp = rh.get_header_properties()
    by_index = rh.get_headers_by_index((0, 3, 5))
    tst1 = list(itemgetter(0, 3, 5)(tmp))
    lkj = rh.get_headers(tst1)
    for f_name in tst1:
        func = getattr(rh, f_name, {})
        if func:
            tst2 = func
    abc = rh.get_headers(["content_json", "accept_json", "cognos_token_header"])
