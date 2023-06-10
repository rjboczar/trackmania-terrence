import os
from typing import Optional
import pandas as pd
import requests
import logging
from asyncio import run as run

# from trackmania import Client, Campaign
from dotenv import load_dotenv
from tokens import auth, user_agent, validate_response

log = logging.getLogger(__name__)
load_dotenv()

# Set your Client User Agent
# Client.USER_AGENT = user_agent


def get_official_maps(force: bool = False, training_maps: bool = True):
    if not force:
        try:
            return pd.read_csv("official_maps.csv")
        except FileNotFoundError:
            print("No official_campaigns.csv found, fetching new data.")
    _, headers = auth("NadeoLiveServices")
    # official Nadeo campaigns
    url_official = f"https://live-services.trackmania.nadeo.live/api/token/campaign/official?offset=0&length=50"
    response_official = requests.get(url_official, headers=headers)
    validate_response(response_official, "Could not get official campaigns")
    campaigns = response_official.json()["campaignList"]
    if training_maps:
        # Should find https://trackmania.io/#/campaigns/19153/3918
        url_training = (
            f"https://live-services.trackmania.nadeo.live/api/token/club/campaign?length=10&offset=0&name"
            f"=TRAINING%20NADEO"
        )
        response_official = requests.get(url_training, headers=headers)
        validate_response(response_official, "Could not get official campaigns")
    maps = []
    # for campaign in response.json()["campaignList"]:
    #     # Get map info by campaign
    #     campaign_map_ids = ",".join([map["mapUid"] for map_ in campaign["playlist"]])
    #     url_maps = f"https://live-services.trackmania.nadeo.live/api/token/map/get-multiple?mapUidList={campaign_map_ids}"
    #     response_maps = requests.get(url_maps, headers=headers)
    #     maps.extend(
    #         [
    #             {
    #                 "campaign": campaign["name"],
    #                 "track": map_["name"],
    #                 "map_id": map_["mapId"],
    #                 "uid": map_["uid"],
    #             }
    #             for map_ in response_maps.json()["mapList"]
    #         ]
    #     )
    return pd.DataFrame(maps)


# async def get_official_campaigns(force: bool = False):
#     if not force:
#         try:
#             return pd.read_csv("official_campaigns.csv")
#         except FileNotFoundError:
#             print("No official_campaigns.csv found, fetching new data.")
#     data = []
#     official_campaigns = await Campaign.official_campaigns()
#     campaigns = [await campaign.get_campaign() for campaign in official_campaigns]
#     # Adds training campaign
#     campaigns.append(await Campaign.get_campaign(3918, 19153))
#     for campaign in campaigns:
#         for map_ in campaign.maps:
#             data.append(
#                 {
#                     "campaign": campaign.name,
#                     "track": map_.name,
#                     "map_id": map_.map_id,
#                     "uid": map_.uid,
#                 }
#             )
#     df = pd.DataFrame(data)
#     df.replace(to_replace="TRAINING NADEO", value="Training", inplace=True)
#     df.to_csv("official_campaigns.csv", index=False)
#     return df


def get_favorite_maps(authors: pd.DataFrame):
    """Gets favorite maps created by authors."""
    _, headers = auth("NadeoLiveServices")
    url = f"https://live-services.trackmania.nadeo.live/api/token/map/favorite?offset=0&length=1000"
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


# def get_maps(authors_df: Optional[pd.DataFrame] = None):
#     maps_df = run(get_official_campaigns())
#     if authors_df is not None:
#         favorite_maps_df = get_favorite_maps(authors_df)
#         maps_df = pd.concat([maps_df, favorite_maps_df], axis=0)
#     maps_df.to_csv("maps.csv", index=False)
#     print(f"Retrieved {len(maps_df)} maps.")
#     return maps_df


if __name__ == "__main__":
    get_official_maps()
