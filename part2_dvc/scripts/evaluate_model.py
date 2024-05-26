# scripts/evaluate_model.py

import json
import os

import joblib
import yaml
import pandas as pd
from sklearn.model_selection import StratifiedKFold, cross_validate


def evaluate_model():
    with open("params.yaml", "r") as fd:
        params = yaml.safe_load(fd)

    with open(
            f'models/{params["ml"]["model_name"]}.{params["ml"]["ext"]}',
            "rb"
        ) as fd:
        pipeline = joblib.load(fd)

    ds = pd.read_csv(
        f'data/{params["ds"]["name"]}.{params["ds"]["ext"]}',
        sep=params['ds']['sep']
    )

    X = ds.drop(params["ds"]["target"], axis=1)
    y = ds[params["ds"]["target"]]

    cv_strategy = StratifiedKFold(n_splits=params["ml"]["n_splits"])
    cv_res = cross_validate(
        pipeline,
        X,
        y,
        cv=cv_strategy,
        n_jobs=params["ml"]["n_jobs"],
        scoring=params["ml"]["metrics"]
    )
    for key, value in cv_res.items():
        cv_res[key] = round(value.mean(), 4)
        if key != "test_r2":
             cv_res[key] = abs(cv_res[key])

    os.makedirs("cv_results", exist_ok=True)
    with open(
            f"cv_results/{params['ml']['model_name']}_cv_res.json",
            "w"
        ) as json_file:
        json.dump(cv_res, json_file)


if __name__ == "__main__":
	evaluate_model()
