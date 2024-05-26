## scripts/load_data.py

import os
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd


def create_connection():
    load_dotenv()

    host = os.environ.get("DB_DESTINATION_HOST")
    port = os.environ.get("DB_DESTINATION_PORT")
    db = os.environ.get("DB_DESTINATION_NAME")
    username = os.environ.get("DB_DESTINATION_USER")
    password = os.environ.get("DB_DESTINATION_PASSWORD")

    conn_string = f"postgresql://{username}:{password}@{host}:{port}/{db}"
    print(conn_string)

    conn = create_engine(conn_string, connect_args={"sslmode":"require"})

    return conn


def load_data():
    with open("params.yaml", "r") as fd:
        params = yaml.safe_load(fd)

    ds_cols = params["ds"]["target"] + params["ds"]["q_features"] + params["ds"]["cat_features"]

    sql_query = f"""
        SELECT *
          FROM yandex_real_estate_data;
    """

    conn = create_connection()
    data = pd.read_sql(sql_query, conn)
    data = data[ds_cols]
    conn.dispose()

    os.makedirs("data", exist_ok=True)
    data.to_csv(
        f"data/{params['ds']['name']}.{params['ds']['ext']}",
        index=None,
        sep=params["ds"]["sep"]
    )


if __name__ == "__main__":
    load_data()
