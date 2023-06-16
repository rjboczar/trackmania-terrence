import base64
import json
from datetime import datetime as dt
import os
from typing import Optional

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


def validate_response(response: requests.Response, error_str: str):
    if response.status_code != 200:
        raise ConnectionError(f"{error_str} :({response.status_code}:{response.text}).")


def validated_get(url: str = "", headers: Optional[dict] = None, error_str: str = ""):
    response = requests.get(url, headers=headers)
    validate_response(response, error_str)
    return response


def token_expiration(full_refresh_token: str) -> dt:
    token = full_refresh_token.split(".")[1]
    token_decoded = base64.urlsafe_b64decode(token + "=" * (4 - len(token) % 4)).decode(
        "utf-8"
    )
    token_d = json.loads(token_decoded)
    return dt.fromtimestamp(token_d["exp"])


def write_token(token_d: dict, audience: str = "NadeoServices"):
    with open(f"tokens/access_token_{audience}.txt", "w") as f:
        f.write(token_d["accessToken"])
    with open(f"tokens/refresh_token_{audience}.txt", "w") as f:
        f.write(token_d["refreshToken"])


def get_token(audience: str = "NadeoServices"):
    # Get ticket from Ubisoft
    ticket_request_url = "https://public-ubiservices.ubi.com/v3/profiles/sessions"
    headers = dict(
        base_headers, **{"Ubi-AppId": "86263886-327a-4328-ac69-527f0d20a237"}
    )
    response = requests.post(
        ticket_request_url,
        headers=headers,
        auth=(os.environ["UBI_USERNAME"], os.environ["UBI_PASSWORD"]),
    )
    validate_response(response, "Could not get ticket")

    # Get tokens from Nadeo
    token_request_url = (
        "https://prod.trackmania.core.nadeo.online/v2/authentication/token/ubiservices"
    )
    headers = dict(
        base_headers, **{"Authorization": f"ubi_v1 t={response.json()['ticket']}"}
    )
    response = requests.post(
        token_request_url, headers=headers, json={"audience": audience}
    )
    validate_response(response, "Could not get token")
    tokens = response.json()
    write_token(tokens, audience)
    log.info("Getting new access token.")
    return tokens["accessToken"]


def refresh_token(audience: str = "NadeoServices"):
    with open(f"tokens/refresh_token_{audience}.txt", "r") as f:
        token = f.read()
    token_refresh_url = (
        "https://prod.trackmania.core.nadeo.online/v2/authentication/token/refresh"
    )
    headers = dict(base_headers, **{"Authorization": f"nadeo_v1 t={token}"})
    response = requests.post(token_refresh_url, headers=headers)
    validate_response(response, "Could not refresh token")

    tokens = response.json()
    exp_time = token_expiration(tokens["refreshToken"])
    log.info(f"Refreshed {audience} token; expires at {exp_time}.")
    write_token(tokens, audience)
    return tokens["accessToken"]


def authenticate(audience: str = "NadeoServices") -> tuple[str, dict]:
    try:
        token = refresh_token(audience)
    except (FileNotFoundError, ConnectionError) as e:
        log.info(f"Error refreshing token: {e}. Getting new token.")
        token = get_token(audience)
    headers = dict(base_headers, **{"Authorization": f"nadeo_v1 t={token}"})
    return token, headers
