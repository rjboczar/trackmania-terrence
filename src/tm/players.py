import pandas as pd
from asyncio import run
import logging

from trackmania import Player, Client

from tm.tokens import user_agent

log = logging.getLogger(__name__)
Client.USER_AGENT = user_agent


async def _get_players(force: bool):
    if not force:
        try:
            return pd.read_csv("data/player_ids.csv")
        except FileNotFoundError:
            log.info("No player_ids.csv found, fetching new data.")
    players = pd.read_csv("data/usernames.csv")
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
        log.info(f"{p.name} - {p.player_id} - {zone}")
        data.append(
            {
                "username": p.name,
                "player_id": p.player_id,
            }
        )
    df = pd.DataFrame(data)
    df.to_csv("data/player_ids.csv", index=False)
    log.info(f"Found {len(df)} players.")
    return df


def get_players(force: bool = False):
    return run(_get_players(force))
