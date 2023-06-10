from typing import Optional
import pandas as pd
import requests
import logging

from dotenv import load_dotenv

from tm.tokens import auth, validate_response

log = logging.getLogger(__name__)
load_dotenv()


def get_official_maps(force: bool = False, training_maps: bool = True):
    if not force:
        try:
            return pd.read_csv("data/official_maps.csv")
        except FileNotFoundError:
            log.info("No official_maps.csv found, fetching new data.")
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
        response_training = requests.get(url_training, headers=headers)
        validate_response(response_training, "Could not get official campaigns")
        training_campaign = next(
            c["campaign"]
            for c in response_training.json()["clubCampaignList"]
            if c["campaignId"] == 3918
        )
        training_campaign["name"] = "Training"
        campaigns.append(training_campaign)
    maps = []
    for campaign in campaigns:
        # Get map info by campaign
        campaign_map_ids = ",".join([map_["mapUid"] for map_ in campaign["playlist"]])
        url_maps = (
            f"https://live-services.trackmania.nadeo.live/api/token/map/"
            f"get-multiple?mapUidList={campaign_map_ids}"
        )
        response_maps = requests.get(url_maps, headers=headers)
        maps.extend(
            [
                {
                    "campaign": campaign["name"],
                    "map_name": map_["name"],
                    "map_id": map_["mapId"],
                    "map_uid": map_["uid"],
                }
                for map_ in response_maps.json()["mapList"]
            ]
        )
    df = pd.DataFrame(maps)
    df.to_csv("data/official_maps.csv", index=False)
    return df


def get_favorite_maps(authors: pd.DataFrame):
    """Gets favorite maps created by authors."""
    _, headers = auth("NadeoLiveServices")
    url = f"https://live-services.trackmania.nadeo.live/api/token/map/favorite?offset=0&length=1000"
    response = requests.get(url, headers=headers)
    validate_response(response, "Could not get favorite maps")
    favorite_df = pd.DataFrame(response.json()["mapList"])
    # Filter to maps created by authors
    favorite_df = pd.merge(
        favorite_df, authors, left_on="author", right_on="player_id", how="inner"
    )[["name", "mapId", "uid"]]
    favorite_df.rename(
        columns={"name": "map_name", "mapId": "map_id", "uid": "map_uid"}, inplace=True
    )

    favorite_df.insert(0, "campaign", "Favorites")
    return favorite_df


def get_maps(training_maps: bool = True, authors: Optional[pd.DataFrame] = None):
    maps_df = get_official_maps(training_maps=training_maps)
    if authors is not None:
        favorite_maps_df = get_favorite_maps(authors)
        maps_df = pd.concat([maps_df, favorite_maps_df], axis=0)
    maps_df.to_csv("data/maps.csv", index=False)
    log.info(f"Retrieved {len(maps_df)} maps.")
    return maps_df
