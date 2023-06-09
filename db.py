import os
import pandas as pd
from sqlalchemy import create_engine, types
from dotenv import load_dotenv

load_dotenv()

username = os.environ["DB_USERNAME"]
password = os.environ["DB_PASSWORD"]
conn_str = os.environ["DB_CONNECTSTRING"]


def update_oracle_db(df: pd.DataFrame):
    engine = create_engine(
        f"oracle+oracledb://{username}:{password}@{conn_str}",
        thick_mode=None,
    )
    dtypes = {c: types.VARCHAR(100) for c in df.columns[df.dtypes == "object"].tolist()}
    df.to_sql(
        "tmp_records",
        engine,
        if_exists="replace",
        index=False,
        dtype=dtypes,
    )
