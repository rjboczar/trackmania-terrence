import pandas as pd
import logging

from tm.nadeo_client import NadeoClient

log = logging.getLogger(__name__)


def get_players(force: bool = False) -> pd.DataFrame:
    """
    Gets player ids from the Trackmania API. Reads from data/teams.csv and writes to data/player_ids.csv.

    :param force: If True, fetch new data.
    :return: pd.DataFrame with columns "username", "player_id", and "team".
    """
    if not force:
        try:
            return pd.read_csv("data/players.csv")
        except FileNotFoundError:
            log.info("No players.csv found, fetching new data from teams.csv.")
    team_data = pd.read_csv("data/teams.csv", header=None, names=("username", "team"))
    client = NadeoClient(audience="OAuth")
    endpoint = "/display-names/account-ids?" + "&".join(
        f"displayName[]={name}" for name in team_data["username"]
    )
    players = client.get_json(endpoint=endpoint)
    df = (
        pd.DataFrame.from_dict(players, orient="index", columns=["player_id"])
        .reset_index()
        .rename(columns={"index": "username"})
    )
    df["team"] = team_data["team"]
    df.to_csv("data/players.csv", index=False)
    log.info(f"Found {len(df)} players.")
    return df
