import json
import pandas as pd


def points_dict(user_times: list[dict[str, str | int]]) -> dict[str, int]:
    """
    Computes points for each user based on their time.

    :param user_times: list of dict with keys 'username' and 'record_time'.
    :return: dict mapping usernames to points.
    """
    # truncate times to seconds digit
    for user_time in user_times:
        user_time["record_time"] //= 1000
    if len(user_times) == 1:
        return {user_times[0]["username"]: 1}
    else:
        # Score = max(0, 3 - (# of players strictly ahead in seconds digit))
        points = {}
        for ut in user_times:
            better_times = sum(
                1 for ut2 in user_times if ut2["record_time"] < ut["record_time"]
            )
            user_points = max(0, 3 - better_times)
            points[ut["username"]] = user_points
        return points


def _untied(points_d: dict[str, int]) -> bool:
    """
    Returns True if the map is untied w.r.t. seconds digit.
    :param points_d: dict mapping usernames to points.
    :return: bool
    """
    # points_d is already sorted, so just check if there aren't two 3s
    return (len(points_d) > 1) and (list(points_d.values())[1] != 3)


def map_stats(group_df: pd.DataFrame) -> pd.Series:
    """
    Computes map stats for a single map.

    :param group_df: DataFrame of map records for a single map, as computed in pd.groupby().
    :return: Series of map stats.
    """
    user_times = (
        group_df.sort_values("record_time")
        .reset_index(drop=True)
        .to_dict(orient="records")
    )
    n = len(user_times)
    stats_ = pd.Series(
        {
            "best_user": user_times[0]["username"],
            "best_time": user_times[0]["record_time"],
            "record_medal": user_times[0]["record_medal"],
            "campaign": user_times[0]["campaign"],
            # Second-fastest user (else '')
            "second_user": user_times[1]["username"] if n > 1 else "",
            # Gap between best and second-best times (else pd.NA)
            "gap": user_times[1]["record_time"] - user_times[0]["record_time"]
            if n > 1
            else pd.NA,
            # Whether more than one user has played the track
            "multi_user": n > 1,
            # dict mapping usernames to points
            "username_points_dict": points_dict(user_times),
        }
    )
    # Whether the map is 'untied' w.r.t. seconds digit
    stats_["untied"] = _untied(stats_["username_points_dict"])
    # Convert points dict to JSON string for DB storage
    stats_["username_points_dict"] = json.dumps(stats_["username_points_dict"])
    return stats_


def compute_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes map stats from map records DataFrame, as computed in records.py.

    :param df: map records DataFrame.
    :return: map stats DataFrame.
    """
    df = (
        df.groupby("map_name", sort=False)[
            ["campaign", "username", "record_time", "record_medal"]
        ]
        .apply(map_stats)
        .reset_index()
    )
    # support for pd.NA (i.e. nullable integer columns) for DB update purposes
    df[["best_time", "record_medal", "gap"]] = df[
        ["best_time", "record_medal", "gap"]
    ].astype("Int64")
    return df
