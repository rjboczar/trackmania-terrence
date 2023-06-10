from datetime import datetime
import pandas as pd
import requests

from tm.db import update_oracle_db
from tm.maps import get_maps
from tm.players import get_players
from tm.tokens import auth


def get_level(map_name: str):
    try:
        return (int(map_name.split(" - ")[1]) - 1) // 5
    except (IndexError, ValueError) as _:
        return -1


def update_records():
    _, headers = auth()
    players = get_players()
    maps = get_maps(authors=players)

    max_maps = 500
    map_data = maps.iloc[:max_maps].reset_index(drop=True)
    player_ids = ",".join(players["player_id"])

    # Need to break up the request into chunks because the URL is too long otherwise
    dfs = []
    chunk_size = 100
    for start in range(0, len(map_data), chunk_size):
        current_map_ids = ",".join(map_data["map_id"][start : start + chunk_size])

        url = (
            f"https://prod.trackmania.core.nadeo.online/mapRecords/?accountIdList={player_ids}"
            f"&mapIdList={current_map_ids}"
        )
        assert len(url) < 8000

        response_maps = requests.get(url, headers=headers)
        records = pd.DataFrame(response_maps.json())
        # Keeps order of map_data
        records = pd.merge(
            map_data, records, left_on="map_id", right_on="mapId", how="left"
        ).dropna(subset=["accountId"])
        records = pd.merge(
            records, players, left_on="accountId", right_on="player_id", how="left"
        )
        records["timestamp"] = pd.to_datetime(records["timestamp"])
        # in ms
        records["record_time"] = records["recordScore"].apply(lambda x: x["time"])
        records["record_medal"] = records["medal"].astype(int)
        records["map_level"] = records["map_name"].apply(get_level)
        records = records[
            [
                "map_id",
                "map_level",
                "map_name",
                "player_id",
                "username",
                "timestamp",
                "record_time",
                "record_medal",
                "campaign",
            ]
        ].reset_index(drop=True)
        dfs.append(records)
    return pd.concat(dfs, axis=0).reset_index(drop=True)


def update():
    df = update_records()
    update_oracle_db(df)
    t = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    print(f"Updated database with {len(df)} records at {t}.")
    df.to_csv(f"records/records_{t}.csv", index=False)
