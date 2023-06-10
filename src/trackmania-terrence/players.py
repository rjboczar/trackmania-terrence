import pandas as pd
from trackmania import Player


async def get_players(force: bool = False):
    if not force:
        try:
            return pd.read_csv("player_ids.csv")
        except FileNotFoundError:
            print("No player_ids.csv found, fetching new data.")
    players = pd.read_csv("usernames.csv")
    data = []
    for username, player_id in zip(players.username, players.player_id):
        result = await Player.search(username)
        if result:
            p = result[0]
        else:
            p = await Player.get_player(player_id)
        if p.zone is False:
            zone = "?"
        else:
            zone = p.zone[0].zone
        print(f"{p.name} - {p.player_id} - {zone}")
        data.append(
            {
                "username": p.name,
                "player_id": p.player_id,
            }
        )
    df = pd.DataFrame(data)
    df.to_csv("player_ids.csv", index=False)
    print(f"Found {len(df)} players.")
    return df
