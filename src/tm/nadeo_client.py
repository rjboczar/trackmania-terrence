from datetime import datetime as dt
import os
import requests
from typing import Optional, Literal
from jwt import decode, ExpiredSignatureError
import logging


from dotenv import load_dotenv

log = logging.getLogger(__name__)
load_dotenv()

user_agent = " / ".join(
    os.environ[k] for k in ("CLIENT_APP", "DISCORD_USERNAME", "CONTACT_EMAIL")
)
base_headers = {"Content-Type": "application/json", "User-Agent": f"{user_agent}"}
base_url_d = {
    "NadeoServices": "https://prod.trackmania.core.nadeo.online",
    "NadeoLiveServices": "https://live-services.trackmania.nadeo.live/api/token",
    "OAuth": "https://api.trackmania.com/api",
}
Audience = Literal["NadeoServices", "NadeoLiveServices", "OAuth"]


def assert_valid_response(response: requests.Response) -> None:
    """Asserts that the response gives a 200 status code."""
    assert response.status_code == 200, f"Request failed: {response.text}"


def decode_token(token: str) -> dict:
    """Decodes a JWT string, verifying expiration but not signature."""
    return decode(
        token,
        options={
            "verify_signature": False,
            "verify_exp": True,
            "verify_iat": True,
            "verify_nbf": True,
        },
    )


def ubisoft_token_auth() -> str:
    """Gets Ubisoft ticket and returns the corresponding encoded JWT string."""
    headers = dict(
        base_headers, **{"Ubi-AppId": "86263886-327a-4328-ac69-527f0d20a237"}
    )
    ticket_response = requests.post(
        url="https://public-ubiservices.ubi.com/v3/profiles/sessions",
        headers=headers,
        auth=(os.environ["UBI_USERNAME"], os.environ["UBI_PASSWORD"]),
    )
    assert_valid_response(ticket_response)
    return ticket_response.json()["ticket"]


def get_request_headers(audience: Audience, auth: Optional[str] = None) -> dict:
    """Returns the headers for a request to the given audience, with header["Authorization"] = auth."""
    if audience == "OAuth":
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
    else:
        headers = base_headers
    return dict(headers, **{"Authorization": auth})


def get_access_token(audience: Audience, refresh_token: Optional[str]) -> str:
    """Gets an access token for the given audience, refreshing if necessary.

    :param audience: The audience for which to get an access token.
    :param refresh_token: The refresh token to use, if any.
    :return: Encoded access token string.
    """
    if refresh_token is not None:
        log.info("Refreshing access token.")
    if audience == "OAuth":
        token_url = "https://api.trackmania.com/api/access_token"
        body = {
            "client_id": os.environ["OAUTH_IDENTIFIER"],
            "client_secret": os.environ["OAUTH_SECRET"],
            "grant_type": "client_credentials",
        }
        auth = None
        post_kwargs = {"data": body}
    else:
        ubisoft_prod = f"{base_url_d['NadeoServices']}/v2/authentication/token"
        if refresh_token is None:
            token_url = f"{ubisoft_prod}/ubiservices"
            auth = f"ubi_v1 t={ubisoft_token_auth()}"
        else:
            token_url = f"{ubisoft_prod}/refresh"
            auth = f"nadeo_v1 t={refresh_token}"
        post_kwargs = {"json": {"audience": audience}}
    headers = get_request_headers(audience=audience, auth=auth)
    response = requests.post(
        url=token_url,
        headers=headers,
        **post_kwargs,
    )
    assert_valid_response(response)
    if audience == "OAuth":
        tokens = {"accessToken": response.json()["access_token"]}
    else:
        tokens = response.json()

    if refresh_token is not None:
        log.info(f"Refreshed {audience} access token.")
    for token_name, token in tokens.items():
        exp = dt.fromtimestamp(decode_token(token)["exp"])
        log.info(f"New {audience} {token_name} expires at {exp}.")
        with open(f"tokens/{token_name}_{audience}.txt", "w") as f:
            f.write(token)
    return tokens["accessToken"]


def get_nadeo_token(audience: Audience) -> str:
    """
    Gets an access token for the given audience.

    :param audience: The audience for which to get an access token.
    :return: Encoded access token string.
    """
    refresh_token = None
    try:
        with open(f"tokens/accessToken_{audience}.txt", "r") as f:
            access_token = f.read()
        try:
            _ = decode_token(access_token)
        except ExpiredSignatureError:
            log.info(f"{audience} access token has expired.")
            if audience != "OAuth":
                try:
                    with open(f"tokens/refreshToken_{audience}.txt", "r") as f:
                        refresh_token = f.read()
                        _ = decode_token(refresh_token)
                except ExpiredSignatureError:
                    log.info(f"{audience} refresh token has expired.")
                    refresh_token = None
        else:
            return access_token
    except FileNotFoundError:
        log.info(f"No {audience} access token found.")
    return get_access_token(audience, refresh_token=refresh_token)


class NadeoClient:
    """Client for making requests to Nadeo APIs.

    :param audience: The audience for which to get an access token."""

    def __init__(self, audience: Audience = "NadeoServices"):
        self.audience = audience
        self.token = get_nadeo_token(audience)  # encoded access token string
        auth_preamble = "nadeo_v1 t=" if self.audience != "OAuth" else "Bearer "
        self.headers = get_request_headers(
            audience=self.audience, auth=f"{auth_preamble}{self.token}"
        )
        self.base_url = base_url_d[audience]

    def get_json(self, endpoint: str):
        """Makes a GET request to the given endpoint and returns the response as JSON.

        The endpoint should be a path relative to the base URL for the given audience.
        :param endpoint: The endpoint to which to make a GET request.
        """
        url = f"{self.base_url}{endpoint}"
        assert len(url) < 8000
        response = requests.get(url=url, headers=self.headers)
        assert_valid_response(response)
        return response.json()
