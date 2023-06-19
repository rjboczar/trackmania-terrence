from typing import Optional
import pandas as pd
import logging

from dotenv import load_dotenv

from tm.nadeo_client import NadeoClient

log = logging.getLogger(__name__)
load_dotenv()


def get_official_maps(
    client: NadeoClient, force: bool = False, training_maps: bool = True
) -> pd.DataFrame:
    """
    Gets map info for official campaigns. Saves to data/official_maps.csv.

    :param client: NadeoClient object with audience="NadeoLiveServices".
    :param force: (bool) If True, force fetching new data.
    :param training_maps: (bool) If True, include training maps.
    :return: pd.DataFrame of official maps.
    """
    if not force:
        try:
            return pd.read_csv("data/official_maps.csv")
        except FileNotFoundError:
            log.info("No official_maps.csv found, fetching new data.")
    # get official campaigns
    campaigns = client.get_json(endpoint="/campaign/official?offset=0&length=50")[
        "campaignList"
    ]
    if training_maps:
        # Should find https://trackmania.io/#/campaigns/19153/3918
        training_campaigns = client.get_json(
            endpoint=f"/club/campaign?length=10&offset=0&name=TRAINING%20NADEO",
        )
        training_campaign = next(
            c["campaign"]
            for c in training_campaigns["clubCampaignList"]
            if c["campaignId"] == 3918
        )
        training_campaign["name"] = "Training"
        campaigns.append(training_campaign)
    maps = []
    for campaign in campaigns:
        # get map info by campaign
        campaign_map_ids = ",".join([map_["mapUid"] for map_ in campaign["playlist"]])
        map_list = client.get_json(
            endpoint=f"/map/get-multiple?mapUidList={campaign_map_ids}",
        )["mapList"]
        maps.extend(
            [
                {
                    "campaign": campaign["name"],
                    "map_name": map_["name"],
                    "map_id": map_["mapId"],
                    "map_uid": map_["uid"],
                }
                for map_ in map_list
            ]
        )
    df = pd.DataFrame(maps)
    df.to_csv("data/official_maps.csv", index=False)
    return df


def get_favorite_maps(
    client: NadeoClient,
    authors: pd.DataFrame,
) -> pd.DataFrame:
    """
    Gets favorite maps filtered by authors DataFrame. Saves to data/favorite_maps.csv.

    :param client: NadeoClient object with audience="NadeoLiveServices".
    :param authors: (pd.DataFrame) Authors DataFrame. Should have column "player_id".
    :return: pd.DataFrame of favorite maps created by authors (has campaign['name'] set as "Favorites").
    """
    map_list = client.get_json(endpoint="/map/favorite?offset=0&length=1000")["mapList"]
    df = pd.DataFrame(map_list)
    # Filter to maps created by authors
    df = pd.merge(df, authors, left_on="author", right_on="player_id", how="inner")[
        ["name", "mapId", "uid"]
    ]
    df.rename(
        columns={"name": "map_name", "mapId": "map_id", "uid": "map_uid"}, inplace=True
    )
    df.insert(0, "campaign", "Favorites")
    return df


def get_maps(
    training_maps: bool = True, authors: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Gets official and favorite maps. Saves to data/maps.csv.

    :param training_maps: (bool) If True, include training maps.
    :param authors: (pd.DataFrame) Authors DataFrame.
      Should have column "player_id". If not None, will include favorite maps filtered by this DataFrame.
    :return: pd.DataFrame of official and favorite maps.
    """
    client = NadeoClient(audience="NadeoLiveServices")
    maps_df = get_official_maps(client, training_maps=training_maps)
    if authors is not None:
        favorite_maps_df = get_favorite_maps(client, authors)
        maps_df = pd.concat([maps_df, favorite_maps_df], axis=0)
    maps_df.to_csv("data/maps.csv", index=False)
    log.info(f"Retrieved {len(maps_df)} maps.")
    return maps_df
