import base64
import json
from datetime import datetime as dt
import os
from functools import partial
from typing import Optional, Callable

import requests
import logging

from dotenv import load_dotenv

log = logging.getLogger(__name__)
load_dotenv()

user_agent = (
    f"{os.environ['CLIENT_APP']} "
    f"/ {os.environ['DISCORD_USERNAME']} "
    f"/ {os.environ['CONTACT_EMAIL']}"
)

base_headers = {
    "Content-Type": "application/json",
    "User-Agent": f"{user_agent}",
}


def _validated_request(
    requests_fn: Callable,
    url: str = "",
    headers: Optional[dict] = None,
    error_str: str = "",
    **kwargs,
):
    response = requests_fn(url, headers=headers, **kwargs)
    if response.status_code != 200:
        raise ConnectionError(f"{error_str} :({response.status_code}:{response.text}).")
    return response


get = partial(_validated_request, requests.get)
post = partial(_validated_request, requests.post)


def _token_expiration(full_refresh_token: str) -> dt:
    token = full_refresh_token.split(".")[1]
    token_decoded = base64.urlsafe_b64decode(token + "=" * (4 - len(token) % 4)).decode(
        "utf-8"
    )
    token_d = json.loads(token_decoded)
    return dt.fromtimestamp(token_d["exp"])


def write_token(token_d: dict, audience: str = "NadeoServices"):
    access_key = "access_token" if audience == "OAuth" else "accessToken"
    with open(f"tokens/access_token_{audience}.txt", "w") as f:
        f.write(token_d[access_key])
    if audience != "OAuth":
        with open(f"tokens/refresh_token_{audience}.txt", "w") as f:
            f.write(token_d["refreshToken"])


def get_token(audience: str = "NadeoServices"):
    if audience == "OAuth":
        token_url = "https://api.trackmania.com/api/access_token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        body = {
            "grant_type": "client_credentials",
            "client_id": os.environ["OAUTH_IDENTIFIER"],
            "client_secret": os.environ["OAUTH_SECRET"],
        }
    else:
        # Get ticket from Ubisoft
        headers = dict(
            base_headers, **{"Ubi-AppId": "86263886-327a-4328-ac69-527f0d20a237"}
        )
        ticket_response = post(
            url="https://public-ubiservices.ubi.com/v3/profiles/sessions",
            headers=headers,
            auth=(os.environ["UBI_USERNAME"], os.environ["UBI_PASSWORD"]),
            error_str="Could not get ticket",
        )
        # Get tokens from Nadeo
        headers = dict(
            base_headers,
            **{"Authorization": f"ubi_v1 t={ticket_response.json()['ticket']}"},
        )
        token_url = "https://prod.trackmania.core.nadeo.online/v2/authentication/token/ubiservices"
        body = {"audience": audience}
    response = post(
        url=token_url,
        headers=headers,
        data=body,
        error_str=f"Could not get {audience} token",
    )
    tokens = response.json()
    write_token(tokens, audience)
    log.info(f"Getting new {audience} access token.")
    return tokens["access_token" if audience == "OAuth" else "accessToken"]


def refresh_token(audience: str = "NadeoServices"):
    if audience == "OAuth":
        raise ConnectionError("Cannot refresh OAuth token")
    with open(f"tokens/refresh_token_{audience}.txt", "r") as f:
        token = f.read()
    token_refresh_url = (
        "https://prod.trackmania.core.nadeo.online/v2/authentication/token/refresh"
    )
    headers = dict(base_headers, **{"Authorization": f"nadeo_v1 t={token}"})
    response = post(
        url=token_refresh_url, headers=headers, error_str="Could not refresh token"
    )
    tokens = response.json()
    exp_time = _token_expiration(tokens["refreshToken"])
    log.info(f"Refreshed {audience} token; expires at {exp_time}.")
    write_token(tokens, audience)
    return tokens["accessToken"]


def authenticate(audience: str = "NadeoServices") -> tuple[str, dict]:
    try:
        token = refresh_token(audience)
    except (FileNotFoundError, ConnectionError) as e:
        log.info(f"Error refreshing token: {e}. Getting new token.")
        token = get_token(audience)
    auth_preamble = "nadeo_v1 t=" if audience != "OAuth" else "Bearer "
    headers = dict(base_headers, **{"Authorization": f"{auth_preamble}{token}"})

    return token, headers
