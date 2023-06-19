import os
import requests
from tm.nadeo_credentials import NadeoCredentials, assert_valid_response


class NadeoOAuthCredentials(NadeoCredentials):
    base_headers: dict = {"Content-Type": "application/x-www-form-urlencoded"}
    refreshable: bool = False

    def __init__(self, **_):
        super().__init__(audience="OAuth")

    def _update_tokens(self, _: bool = False) -> dict:
        """Gets a token pair for the given audience, refreshing if necessary.

        :return: dict with keys "access" and "refresh" and corresponding token strings.
        """
        response = requests.post(
            url="https://api.trackmania.com/api/access_token",
            headers=self.base_headers,
            data={
                "client_id": os.environ["OAUTH_IDENTIFIER"],
                "client_secret": os.environ["OAUTH_SECRET"],
                "grant_type": "client_credentials",
            },
        )
        assert_valid_response(response)
        return {"access": response.json()["access_token"]}
