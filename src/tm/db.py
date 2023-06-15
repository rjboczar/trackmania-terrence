import logging
import os
import pandas as pd
from sqlalchemy import create_engine, types, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

log = logging.getLogger(__name__)

load_dotenv()

username = os.environ["DB_USERNAME"]
password = os.environ["DB_PASSWORD"]
conn_str = os.environ["DB_CONNECTSTRING"]


def _engine():
    return create_engine(
        f"oracle+oracledb://{username}:{password}@{conn_str}",
        thick_mode=None,
        pool_pre_ping=True,
    )


def update_oracle_db(dfs: dict[str, pd.DataFrame]) -> bool:
    engine = _engine()
    try:
        engine.connect()
    except OperationalError as _:
        log.error(f"Couldn't connect to db.")
        return False

    for db_name, df in dfs.items():
        dtypes = {
            c: types.VARCHAR(200) for c in df.columns[df.dtypes == "object"].tolist()
        }
        df.to_sql(
            db_name,
            engine,
            if_exists="replace",
            index=False,
            dtype=dtypes,
        )
    return True


if __name__ == "__main__":
    print(pd.read_sql_query(text("SELECT * FROM map_stats"), _engine().connect()))
