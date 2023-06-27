from datetime import datetime
import pandas as pd

from tm.db import update_oracle_db
from tm.maps import get_maps
from tm.players import get_players
from tm.nadeo_client import NadeoClient
from tm.stats import map_stats, map_points


def get_level(map_name: str) -> int:
    """
    Returns the level of the map based on the map name. Ranges from 0 to 4, with -1 for unknown.

    :param map_name: str
    :return: int
    """
    try:
        return (int(map_name.split(" - ")[1]) - 1) // 5
    except (IndexError, ValueError) as _:
        return -1


def map_records() -> dict[str, pd.DataFrame]:
    """
    Gets map records for all players returned by get_players().
    Gets the records for official campaigns and favorite maps created by the players in get_players().

    :return: 'map_records' and 'map_stats' DataFrames.
    """
    players = get_players()
    maps = get_maps(authors=players)

    max_maps = 1000
    map_data = maps.iloc[:max_maps].reset_index(drop=True)
    map_data["map_level"] = map_data["map_name"].apply(get_level)
    player_ids = ",".join(players["player_id"])

    # Need to break up the request into chunks because the URL is too long otherwise
    dfs = []
    chunk_size = 200
    client = NadeoClient(audience="NadeoServices")

    for start in range(0, len(map_data), chunk_size):
        current_map_ids = ",".join(map_data["map_id"][start : start + chunk_size])
        endpoint = (
            f"/mapRecords/?accountIdList={player_ids}&mapIdList={current_map_ids}"
        )
        records = pd.DataFrame(client.get_json(endpoint=endpoint))
        # keeps records in order of map_data
        records = pd.merge(
            map_data, records, left_on="map_id", right_on="mapId", how="left"
        ).dropna(subset=["accountId"])
        records = pd.merge(
            records, players, left_on="accountId", right_on="player_id", how="left"
        )
        records["timestamp"] = pd.to_datetime(records["timestamp"])
        # record_time is in ms
        records["record_time"] = records["recordScore"].apply(lambda x: x["time"])
        records["record_medal"] = records["medal"].astype(int)
        records = records[
            [
                "map_id",
                "map_level",
                "map_name",
                "player_id",
                "username",
                "team",
                "timestamp",
                "record_time",
                "record_medal",
                "campaign",
            ]
        ].reset_index(drop=True)
        dfs.append(records)
    df = pd.concat(dfs, axis=0).reset_index(drop=True)
    # keeps current order of maps and sorts by record_time increasing
    df = df.groupby("map_id").apply(map_points).reset_index(drop=True)
    # best times and records for each map
    map_stats_df = map_stats(df)
    # Join back the map data on map_stats_df, so maps with no records are still included
    map_stats_df = pd.merge(
        map_data[["map_name", "campaign"]],
        map_stats_df,
        on=["map_name", "campaign"],
        how="left",
    )
    # replace NaN based on type (Oracle treats empty string as null)
    bool_cols = ["multi_user", "untied"]
    map_stats_df[bool_cols] = map_stats_df[bool_cols].fillna(pd.NA).astype("boolean")
    str_cols = map_stats_df.columns[map_stats_df.dtypes == "object"]
    map_stats_df[str_cols] = map_stats_df[str_cols].fillna("")

    # Compute campaign stats for total team points and mvp
    campaign_teams = pd.merge(
        players[["team", "username"]], map_stats_df["campaign"], how="cross"
    ).drop_duplicates()
    campaign_points = (
        df.replace({"": pd.NA})
        .groupby(["campaign", "username"], sort=False)[["points"]]
        .sum()
        .reset_index()
    )
    campaign_points = pd.merge(
        campaign_teams,
        campaign_points,
        on=("campaign", "username"),
        how="left",
        sort=False,
    )
    campaign_points["points"] = campaign_points["points"].fillna(0).astype(int)
    campaign_stats_df = (
        campaign_points.sort_values("points", ascending=False)
        .groupby(["campaign", "team"])
        .agg(
            points=("points", "sum"),
            mvp=("username", "first"),
            mvp_points=("points", "first"),
        )
    ).reset_index()

    campaign_stats_df.loc[campaign_stats_df["points"] == 0, "mvp"] = ""

    return {
        "map_records": df,
        "map_stats": map_stats_df,
        "campaign_stats": campaign_stats_df,
        "player_data": players,
    }


def update() -> bool:
    """
    Updates the database with the latest map records and stats.
    Saves the records and stats DataFrames to CSV files in records/.
    :return: (bool) True if successful.
    """
    dfs = map_records()
    ok = update_oracle_db(dfs)
    if ok:
        t = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        print(f"Updated database with {len(dfs['map_records'])} records at {t}.")
        dfs["map_records"].to_csv(f"records/map_records_{t}.csv", index=False)
        dfs["map_stats"].to_csv(f"records/map_stats_{t}.csv", index=False)
    return ok
