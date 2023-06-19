import logging
import os
import pandas as pd
import numpy as np
from oracledb import connect, DatabaseError
from dotenv import load_dotenv

log = logging.getLogger(__name__)

load_dotenv()


def _oracle_dtype(col: pd.Series) -> str:
    """
    Maps a pandas Series to an Oracle dtype.
    :param col: dtype of the Series.
    :return: Oracle dtype string cor use in create table.
    """
    dtype = col.dtype.name
    if dtype in ("int64", "Int64", "boolean"):
        return "NUMBER"
    elif dtype == "object":
        return f"VARCHAR2(200)"
    elif dtype == "datetime64[ns, UTC]":
        return "TIMESTAMP WITH TIME ZONE"
    else:
        raise ValueError(f"Unknown dtype {dtype}.")


def update_oracle_db(dfs: dict[str, pd.DataFrame]) -> bool:
    """
    Update the Oracle DB with the dataframes in the dict.
    :param dfs: dict of DataFrames to update the Oracle DB with. Maps DB name (str) to pd.DataFrame.
    :return: bool indicating whether the update was successful.
    """
    username = os.environ["DB_USERNAME"]
    password = os.environ["DB_PASSWORD"]
    conn_str = os.environ["DB_CONNECTSTRING"]
    try:
        with connect(dsn=conn_str, user=username, password=password) as connection:
            with connection.cursor() as cursor:
                for db_name, df in dfs.items():
                    try:
                        cursor.execute(f"drop table {db_name}")
                    except DatabaseError:
                        pass
                    # Create table using the correct types for Oracle DB
                    cols = df.columns
                    schema = ",".join(f"{c} {_oracle_dtype(df[c]) }" for c in cols)
                    cursor.execute(f"create table {db_name} ({schema})")
                    # Insert data into table, imputing NaN with None per docs
                    binds = ", ".join(f":{c}" for c in df.columns)
                    cursor.executemany(
                        f"insert into {db_name}"
                        f"({', '.join(cols)}) values ({binds})",
                        df.replace([np.nan], [None]).to_dict("records"),
                    )
                    connection.commit()
    except DatabaseError as e:
        log.error(f"Error connecting to Oracle DB: {e}")
        return False
    return True
