import os
import pandas as pd
from sqlalchemy import create_engine, types, text
from dotenv import load_dotenv

load_dotenv()

username = os.environ["DB_USERNAME"]
password = os.environ["DB_PASSWORD"]
conn_str = os.environ["DB_CONNECTSTRING"]


def update_oracle_db(dfs: dict[str, pd.DataFrame]):
    engine = create_engine(
        f"oracle+oracledb://{username}:{password}@{conn_str}",
        thick_mode=None,
    )
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


if __name__ == "__main__":
    engine = create_engine(
        f"oracle+oracledb://{username}:{password}@{conn_str}",
        thick_mode=None,
    )
    df_ = pd.read_sql_query(text("SELECT * FROM map_stats"), engine.connect())
    print(df_.dtypes)
    print(df_["best_time"])
    # print(df)
