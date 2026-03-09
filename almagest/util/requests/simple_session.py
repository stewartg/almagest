import os

import requests
from requests.adapters import HTTPAdapter, Retry

from almagest.util.requests.requests_header_helper import RequestsHeaderHelper


class SimpleSession(requests.Session):
    """A simple wrapper for a requests session.

    It sets up retries and a backoff factor for the session and provides easy
    access to common request headers as properties.
    """

    def __init__(self) -> None:
        super().__init__()
        self.hdr_helper = RequestsHeaderHelper()

    def add_refresh_token_hook(self):
        """Provides a way to add retry logic when retrieving an auth token."""
        retry_cnt = os.getenv("SESSION_RETRIES", "")
        retries = Retry(total=int(retry_cnt), backoff_factor=1, status_forcelist=[502, 503, 504])
        self.mount("http://", HTTPAdapter(max_retries=retries))
        self.mount("https://", HTTPAdapter(max_retries=retries))
        self.cert = self.hdr_helper.certs
        self.verify = False
        self.hooks["response"].append(self.refresh_token_auth)

    def refresh_token_auth(self, res, *args, **kwargs):
        """Forces the security token to be re-issued if the response is unauthorized or forbidden."""
        stat_code = res.status_code
        # pylint: disable=E1101
        if stat_code == requests.codes.UNAUTHORIZED or stat_code == requests.codes.FORBIDDEN:
            tmp_max_token_age = self.hdr_helper.max_token_age_sec
            self.hdr_helper.max_token_age_sec = 0
            self.headers.update({"Authorization": f"Bearer {self.hdr_helper.security_token}"})
            self.hdr_helper.max_token_age_sec = tmp_max_token_age

    @property
    def content_token_headers(self) -> dict:
        """Gets the content_token_headers from the header helper.

        :return: a dictionary that contains the content type and cognos token
                headers.
        """
        return {**self.hdr_helper.content_json, **self.hdr_helper.bearer_auth}

    @property
    def bearer_auth_header(self):
        """Gets the cognos_token_header from the header helper.

        :return: a dictionary that contains the cognos token header.
        """
        return self.hdr_helper.bearer_auth

    @property
    def accept_token_headers(self):
        """Gets the accept_token_headers from the header helper.

        :return: a dictionary that contains the accept json and cognos token
                headers.
        """
        return {**self.hdr_helper.accept_json, **self.hdr_helper.bearer_auth}

    @property
    def cognos_client_headers(self):
        """Gets the cognos_client_headers from the header helper.

        :return: a dictionary that contains the cognos client headers.
        """
        return self.hdr_helper.cognos_client_headers

    @property
    def certs(self):
        """Gets the cognos npe certs from the header helper.

        :return: a dictionary that contains the certs header.
        """
        return self.hdr_helper.certs
