import pandas as pd


def _untied(points_d: dict[str, int]) -> bool:
    """
    Returns True if the map is untied w.r.t. seconds digit.
    :param points_d: dict mapping usernames to points.
    :return: bool
    """
    # points_d is already sorted, so just check if there aren't two 3s
    return (len(points_d) > 1) and (list(points_d.values())[1] != 3)


def single_map_stats(group_df: pd.DataFrame) -> pd.Series:
    """
    Computes map stats for a single map.

    :param group_df: DataFrame of map records for a single map, as computed in pd.groupby().
    :return: Series of map stats.
    """
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
        }
    ).rename({"record_time": "best_time"})
    # Whether the map is 'untied' w.r.t. seconds digit
    stats_["untied"] = not multi_user or (
        group_df["points"].iloc[0] > group_df["points"].iloc[1]
    )
    stats_["points_str"] = ",".join(group_df["points"].astype(str))
    return stats_


def map_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes map stats from map records DataFrame, as computed in records.py.

    :param df: map records DataFrame.
    :return: map stats DataFrame.
    """
    df = (
        df.groupby("map_name", sort=False)[
            ["campaign", "username", "record_time", "record_medal", "points"]
        ]
        .apply(single_map_stats)
        .reset_index()
    )
    # support for pd.NA (i.e. nullable integer columns) for DB update purposes
    int_cols = ["best_time", "record_medal", "gap", "points"]
    df[int_cols] = df[int_cols].astype("Int64")
    return df


def map_points(group_df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes map points from groupby object for each player.

    Score = 3 - (# of players strictly ahead in seconds digit)
    If there are 1 or 2 players, scores are decreased by 2 or 1 points, respectively.

    :param group_df: DataFrame for a single map_id.
    :return: DataFrame sorted by record time with additional column "points".
    """
    group_df = group_df.sort_values("record_time")
    # truncate times to seconds digit
    time_s = group_df["record_time"] // 1000
    points = []
    current = None
    rank = -1
    penalty = max(3 - len(time_s), 0)
    for ix, val in enumerate(time_s):
        if current != val:
            current = val
            rank = ix
        points.append(max(3 - rank - penalty, 0))
    group_df["points"] = points
    return group_df
