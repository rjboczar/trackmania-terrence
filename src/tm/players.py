import pandas as pd
import logging

from tm.nadeo_client import NadeoClient

log = logging.getLogger(__name__)


def get_players(force: bool = False) -> pd.DataFrame:
    """
    Gets player ids from the Trackmania API. Reads from data/display_names.csv and writes to data/player_ids.csv.

    :param force: If True, fetch new data.
    :return: pd.DataFrame with columns "username" and "player_id".
    """
    if not force:
        try:
            return pd.read_csv("data/player_ids.csv")
        except FileNotFoundError:
            log.info("No player_ids.csv found, fetching new data.")
    display_names = pd.read_csv("data/display_names.csv", header=None)
    client = NadeoClient(audience="OAuth")
    endpoint = "/display-names/account-ids?" + "&".join(
        f"displayName[]={name}" for name in display_names[0]
    )
    players = client.get_json(endpoint=endpoint)
    df = (
        pd.DataFrame.from_dict(players, orient="index", columns=["player_id"])
        .reset_index()
        .rename(columns={"index": "username"})
    )
    df.to_csv("data/player_ids.csv", index=False)
    log.info(f"Found {len(df)} players.")
    return df
