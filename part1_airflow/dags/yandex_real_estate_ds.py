# dags/yandex_real_estate_ds.py

import pendulum
from airflow.decorators import dag, task
from callbacks.messages import *
from preprocessing.anomalies_detection import *


@dag(
    dag_id="yandex_real_estate_ds",
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    on_failure_callback=send_telegram_failure_message,
    on_success_callback=send_telegram_success_message,
    catchup=False,
    tags=["ETL", "project1", "yandex", "real_estate"]
)
def yandex_real_estate_ds():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook


    @task()
    def create_table():
        from sqlalchemy import Table, Column, Float, Integer, MetaData, \
            String, UniqueConstraint, inspect
        

        hook = PostgresHook("destination_db")
        db_engine = hook.get_sqlalchemy_engine()
        metadata = MetaData()

        churn_table = Table(
            "yandex_real_estate_data",
            metadata,

            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("building_id", String),
            Column("flat_id", String),

            Column("build_year", Integer),
            Column("building_type_int", String),
            Column("latitude", Float),
            Column("longitude", Float),
            Column("ceiling_height", Float),
            Column("flats_count", Integer),
            Column("floors_total", Integer),
            Column("has_elevator", Integer),
            Column("floor", Integer),
            Column("kitchen_area", Float),
            Column("living_area", Float),
            Column("rooms", Integer),
            Column("is_apartment", Integer),
            Column("studio", Integer),
            Column("total_area", Float),
            Column("price", Float),

            UniqueConstraint(
                "id",
                name="unique_flat_id_constraint"
            )
        )
        if not inspect(db_engine).has_table(churn_table.name):
            metadata.create_all(db_engine)

    
    @task()
    def extract():
        from sqlalchemy import MetaData, Table, select


        hook = PostgresHook("destination_db")
        conn = hook.get_conn()
        db_engine = hook.get_sqlalchemy_engine()
        metadata = MetaData()

        buildings = Table(
            "buildings", metadata, autoload=True, autoload_with=db_engine)
        flats = Table(
            "flats", metadata, autoload=True, autoload_with=db_engine)

        query = (
            select(
                [
                    flats.c.building_id,
                    flats.c.id.label('flat_id'),
                    buildings.c.build_year,
                    buildings.c.building_type_int,
                    buildings.c.latitude,
                    buildings.c.longitude,
                    buildings.c.ceiling_height,
                    buildings.c.flats_count,
                    buildings.c.floors_total,
                    buildings.c.has_elevator,
                    flats.c.floor,
                    flats.c.kitchen_area,
                    flats.c.living_area,
                    flats.c.rooms,
                    flats.c.is_apartment,
                    flats.c.studio,
                    flats.c.total_area,
                    flats.c.price
                ]
            ).select_from(
                buildings
                .outerjoin(
                    flats,
                    buildings.c.id == flats.c.building_id
                )
            )
        )

        with db_engine.connect() as conn:
            data = pd.read_sql(query, conn)
        conn.close()

        return data


    @task()
    def transform(data: pd.DataFrame):
        ids_colums = [col for col in data.columns if col.endswith("id")]
        b_features = ["has_elevator", "is_apartment", "studio"]
        features2drop = ["studio", "is_anomaly"]

        data[b_features] = data[b_features].astype("int64")
        
        data = data.drop_duplicates(
            subset=data.columns.drop(ids_colums)
        )
        
        data = (
            data[
                (data["price"] >= 1e+6)
                & (data["price"] <= 1.5e+8)
            ]
            .reset_index(drop=True)
        )

        data["is_anomaly"] = 0
        for col in data.columns.drop(ids_colums + ["building_type_int"]):
            up_bound, low_bound = get_iqr_bounds(data, col)
            data["is_anomaly"] = np.where(
                data[col].between(low_bound, up_bound),
                data["is_anomaly"],
                1
            )

        data = data[data["is_anomaly"] == 0].reset_index(drop=True)
        data = data.drop(features2drop, axis=1)
  
        return data


    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        db_engine = hook.get_sqlalchemy_engine()

        with db_engine.connect() as conn:
            result = conn.execute(
                "SELECT COUNT(*) FROM yandex_real_estate_data;"
            )
            count = result.scalar()

        if count == 0:
            hook.insert_rows(
                table='yandex_real_estate_data',
                replace=True,
                target_fields=data.columns.tolist(),
                replace_index=['id'],
                rows=data.values.tolist()
            )
        else:
            print("Table is not empty, no data loaded")


    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)


yandex_real_estate_ds()
