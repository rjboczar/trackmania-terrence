import os
from typing import Optional
import pandas as pd
import requests
from asyncio import run as run

from trackmania import Client, Campaign
from dotenv import load_dotenv
from tokens import auth

load_dotenv()

# Set your Client User Agent
Client.USER_AGENT = f"{os.environ['DISCORD_USERNAME']} | {os.environ['CLIENT_APP']}"


async def get_official_campaigns(force: bool = False):
    if not force:
        try:
            return pd.read_csv("official_campaigns.csv")
        except FileNotFoundError:
            print("No official_campaigns.csv found, fetching new data.")
    data = []
    official_campaigns = await Campaign.official_campaigns()
    campaigns = [await campaign.get_campaign() for campaign in official_campaigns]
    # Adds training campaign
    campaigns.append(await Campaign.get_campaign(3918, 19153))
    for campaign in campaigns:
        for map_ in campaign.maps:
            data.append(
                {
                    "campaign": campaign.name,
                    "track": map_.name,
                    "map_id": map_.map_id,
                    "uid": map_.uid,
                }
            )
    df = pd.DataFrame(data)
    df.replace(to_replace="TRAINING NADEO", value="Training", inplace=True)
    df.to_csv("official_campaigns.csv", index=False)
    return df


def get_favorite_maps(authors: pd.DataFrame):
    """Gets favorite maps created by authors."""
    token = auth("NadeoLiveServices")
    url = f"https://live-services.trackmania.nadeo.live/api/token/map/favorite?offset=0&length=1000"
    headers = {
        "Authorization": f"nadeo_v1 t={token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    response = requests.get(url, headers=headers)
    favorite_df = pd.DataFrame(response.json()["mapList"])
    # Filter to maps created by authors
    favorite_df = pd.merge(
        favorite_df, authors, left_on="author", right_on="player_id", how="inner"
    )
    favorite_df = favorite_df[["name", "mapId", "uid"]]
    favorite_df.insert(0, "campaign", "Terrence")
    favorite_df.rename(
        columns={
            "name": "track",
            "mapId": "map_id",
        },
        inplace=True,
    )
    return favorite_df


def get_maps(authors_df: Optional[pd.DataFrame] = None):
    maps_df = run(get_official_campaigns())
    if authors_df is not None:
        favorite_maps_df = get_favorite_maps(authors_df)
        maps_df = pd.concat([maps_df, favorite_maps_df], axis=0)
    maps_df.to_csv("maps.csv", index=False)
    print(f"Retrieved {len(maps_df)} maps.")
    return maps_df
