# scripts/fit_bs_model.py

import yaml
import os
import joblib
import pandas as pd

from category_encoders import CatBoostEncoder

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler
from sklearn.linear_model import LinearRegression


def fit_model():
    with open("params.yaml", "r") as fd:
        params = yaml.safe_load(fd)

    ds = pd.read_csv(
        f"data/{params['ds']['name']}.{params['ds']['ext']}",
        sep="|"
    )

    model = LinearRegression()

    preprocessor = ColumnTransformer(
        [
            (
                "cat_features",
                CatBoostEncoder(return_df=False),
                params["ds"]["cat_features"]
            ),
            (
                "q_features",
                MinMaxScaler(),
                params["ds"]["q_features"]
            )
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )

    pipeline = Pipeline(
        [
            (
                'preprocessor',
                preprocessor
            ),
            (
                'model',
                model
            )
        ]
    )

    X = ds.drop(params["ds"]["target"], axis=1)
    y = ds[params["ds"]["target"]]

    pipeline.fit(X, y)

    os.makedirs('models', exist_ok=True)
    with open(
        f'models/{params["ml"]["model_name"]}.{params["ml"]["ext"]}',
        'wb'
    ) as fd:
        joblib.dump(pipeline, fd)


if __name__ == '__main__':
	fit_model()
