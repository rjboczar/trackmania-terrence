import json
import pandas as pd


def points_dict(user_times: pd.DataFrame) -> dict[str, int]:
    """
    Computes points for each user based on their time.

    Score = 3 - (# of players strictly ahead in seconds digit)
    If there are 1 or 2 players, scores are decreased by 2 or 1 points, respectively.
    :param user_times: pd.DataFrame with keys 'username' and 'record_time'. Times are in milliseconds.
    :return: dict mapping usernames to points.
    """
    # truncate times to seconds digit
    user_times["record_time"] //= 1000
    points = dict()
    current = None
    rank = -1
    penalty = max(3 - len(user_times), 0)
    for ix, (username, val) in enumerate(
        zip(user_times["username"], user_times["record_time"])
    ):
        if current != val:
            current = val
            rank = ix
        points[username] = max(3 - rank - penalty, 0)
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
    group_df = group_df.sort_values("record_time")
    n = len(group_df)
    multi_user = n > 1
    best = group_df.iloc[0]
    second_best = group_df.iloc[1] if n > 1 else None
    stats_ = pd.Series(
        {
            **best,
            # Second-fastest user (else '')
            "second_user": second_best["username"] if multi_user else "",
            # Gap between best and second-best times (else pd.NA)
            "gap": second_best["record_time"] - best["record_time"]
            if multi_user
            else pd.NA,
            # Whether more than one user has played the track
            "multi_user": multi_user,
            # dict mapping usernames to points
            "username_points_dict": points_dict(group_df),
        }
    ).rename({"record_time": "best_time"})
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
