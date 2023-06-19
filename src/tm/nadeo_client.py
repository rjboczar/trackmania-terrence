import requests

from tm.nadeo_credentials import NadeoCredentials, assert_valid_response
from tm.nadeo_oauth_credentials import NadeoOAuthCredentials
import logging


from dotenv import load_dotenv

log = logging.getLogger(__name__)
load_dotenv()


class NadeoClient:
    """Client for making requests to Nadeo APIs.

    :param audience: The audience for which to get an access token."""

    def __init__(self, audience: str = "NadeoServices"):
        self.audience = audience
        if audience == "OAuth":
            self.creds = NadeoOAuthCredentials()
            auth_preamble = "Bearer "
        else:
            self.creds = NadeoCredentials(audience)
            auth_preamble = "nadeo_v1 t="
        self.headers = self.creds.request_headers(
            auth=f"{auth_preamble}{self.creds.tokens['access']}"
        )

    def get_json(self, endpoint: str):
        """Makes a GET request to the given endpoint and returns the response as JSON.

        The endpoint should be a path relative to the base URL for the given audience.
        :param endpoint: The endpoint to which to make a GET request.
        """
        url = f"{self.creds.base_url}{endpoint}"
        assert len(url) < 8000
        response = requests.get(url=url, headers=self.headers)
        assert_valid_response(response)
        return response.json()
