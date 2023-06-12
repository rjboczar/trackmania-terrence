import json
import pandas as pd


def points_dict(user_times: dict):
    # truncate times to s
    for user_time in user_times:
        user_time["record_time"] //= 1000
    if len(user_times) == 1:
        return {user_times[0]["username"]: 1}
    else:
        # Score = max(0, 3 - (# of players strictly ahead in seconds digit)
        # i.e. the-elite scoring
        points = {}
        for ut in user_times:
            better_times = sum(
                1 for ut2 in user_times if ut2["record_time"] < ut["record_time"]
            )
            user_points = max(0, 3 - better_times)
            points[ut["username"]] = user_points
        return points


def map_stats(group_df: pd.DataFrame):
    user_times = (
        group_df.sort_values("record_time")
        .reset_index(drop=True)
        .to_dict(orient="records")
    )
    n = len(user_times)
    return pd.Series(
        {
            "best_user": user_times[0]["username"],
            "best_time": user_times[0]["record_time"],
            "record_medal": user_times[0]["record_medal"],
            "campaign": user_times[0]["campaign"],
            "second_user": user_times[1]["username"] if n > 1 else "",
            "gap": user_times[1]["record_time"] - user_times[0]["record_time"]
            if n > 1
            else -1,
            "multi_user": n > 1,
            "username_points_dict": json.dumps(points_dict(user_times)),
        }
    )


def compute_stats(df: pd.DataFrame):
    return (
        df.groupby("map_name", sort=False)[
            ["campaign", "username", "record_time", "record_medal"]
        ]
        .apply(map_stats)
        .reset_index()
    )
