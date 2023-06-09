import base64
import json
import datetime
import os
import requests

from dotenv import load_dotenv

load_dotenv()


def token_expiration(full_refresh_token: str) -> datetime.datetime:
    s = full_refresh_token.split(".")[1]
    s_decoded = base64.urlsafe_b64decode(s + "=" * (4 - len(s) % 4)).decode("utf-8")
    s_d = json.loads(s_decoded)
    return datetime.datetime.fromtimestamp(s_d["exp"])


def write_token(token_d: dict, audience: str = "NadeoServices"):
    with open(f"access_token_{audience}.txt", "w") as f:
        f.write(token_d["accessToken"])
    with open(f"refresh_token_{audience}.txt", "w") as f:
        f.write(token_d["refreshToken"])


def get_token(audience: str = "NadeoServices"):
    url = "https://public-ubiservices.ubi.com/v3/profiles/sessions"
    with open("auth.csv", "r") as f:
        username, password = f.read().split(",")
    headers = {
        "Content-Type": "application/json",
        "Ubi-AppId": "86263886-327a-4328-ac69-527f0d20a237",
        "User-Agent": f"{os.environ['ClientApp']} / {username}",
    }
    response = requests.post(url, headers=headers, auth=(username, password))

    url2 = (
        "https://prod.trackmania.core.nadeo.online/v2/authentication/token/ubiservices"
    )
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"ubi_v1 t={response.json()['ticket']}",
    }
    response_token = requests.post(url2, headers=headers, json={"audience": audience})
    if response_token.status_code != 200:
        raise ConnectionError("Could not get token.")
    tokens = response_token.json()
    write_token(tokens, audience)
    print("Getting new access token.")
    return tokens["accessToken"]


def refresh_token(audience: str = "NadeoServices"):
    refresh_url = (
        "https://prod.trackmania.core.nadeo.online/v2/authentication/token/refresh"
    )
    with open(f"refresh_token_{audience}.txt", "r") as f:
        token = f.read()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"nadeo_v1 t={token}",
    }
    response = requests.post(refresh_url, headers=headers)
    if response.status_code != 200:
        raise ConnectionError("Could not refresh token.")
    tokens = response.json()
    exp_time = token_expiration(tokens["refreshToken"])
    print(f"Refreshed {audience} token; expires at {exp_time}.")
    write_token(tokens, audience)
    return tokens["accessToken"]


def auth(audience: str = "NadeoServices"):
    try:
        return refresh_token(audience)
    except (FileNotFoundError, ConnectionError) as _:
        return get_token(audience)
