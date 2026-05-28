#!/usr/bin/env python3
"""
train_ml_model.py

Trains 4 XGBoost regressors (one per forward-return horizon: 3m / 6m / 9m / 12m)
on mart_ml_features, then writes predictions for every ticker+date row to
RAW_ML_PREDICTIONS in Snowflake PUBLIC schema.

The nightly dbt run then builds mart_ml_predictions and mart_model_evaluation
on top of these raw predictions.

Run locally:
    cd DBTSTOCKPROJECT_SNOWFLAKE
    set -a && source DBTSTOCKPROJECT/.env && set +a
    python3 Python_Scripts/train_ml_model.py

Env vars required:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER,
    SNOWFLAKE_PRIVATE_KEY_PATH  (preferred) or SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE
"""

import logging
import os
import warnings
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import snowflake.connector
import xgboost as xgb
from sklearn.impute import SimpleImputer
from snowflake.connector.pandas_tools import write_pandas

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Configuration ─────────────────────────────────────────────────────────────

# Columns excluded from the feature matrix (identifiers, raw price levels, targets)
EXCLUDE_COLS = frozenset({
    "ticker", "date", "company_name", "sector", "industry", "country",
    "asset_class",
    # raw price/volume levels — normalised versions (ratios) are kept
    "close", "adjusted_close", "volume",
    "week_52_high", "week_52_low",
    "shares_outstanding", "shares_float",
    # forward-return targets
    "forward_return_1m", "forward_return_3m", "forward_return_6m",
    "forward_return_9m", "forward_return_12m",
    # target-availability flags (derived from targets — pure leakage)
    "has_1m_target", "has_3m_target", "has_6m_target",
    "has_9m_target", "has_12m_target",
    # split labels
    "dataset_split", "dataset_split_9m", "dataset_split_12m",
})

# Each horizon maps to its target column and the split column that labels
# which rows have a materialised target (= valid training rows).
HORIZONS = {
    "3m":  {"target": "forward_return_3m",  "split_col": "dataset_split"},
    "6m":  {"target": "forward_return_6m",  "split_col": "dataset_split"},
    "9m":  {"target": "forward_return_9m",  "split_col": "dataset_split_9m"},
    "12m": {"target": "forward_return_12m", "split_col": "dataset_split_12m"},
}

XGB_PARAMS = {
    "n_estimators":     400,
    "max_depth":        5,
    "learning_rate":    0.05,
    "subsample":        0.8,
    "colsample_bytree": 0.7,
    "min_child_weight": 25,   # guards against overfitting on thin cross-sections
    "reg_alpha":        0.1,
    "reg_lambda":       1.0,
    "random_state":     42,
    "n_jobs":          -1,
    "tree_method":     "hist",
    "verbosity":        0,
}

TARGET_TABLE   = "RAW_ML_PREDICTIONS"
TARGET_SCHEMA  = "PUBLIC"
TARGET_DATABASE = "DBT_STOCKPROJECT"
FEATURE_TABLE  = f"{TARGET_DATABASE}.JDS_MARTS.JDS_MART_MART_ML_FEATURES"


# ── Snowflake connection ───────────────────────────────────────────────────────

def get_connection() -> snowflake.connector.SnowflakeConnection:
    kwargs = {
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "role":      os.environ.get("SNOWFLAKE_ROLE",      "DBT_ROLE"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "DBT_STOCKPROJECT"),
        "database":  os.environ.get("SNOWFLAKE_DATABASE",  TARGET_DATABASE),
        "schema":    TARGET_SCHEMA,
    }
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        kwargs["private_key_file"] = key_path
        kwargs["authenticator"]    = "snowflake_jwt"
    else:
        kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    return snowflake.connector.connect(**kwargs)


# ── Data loading ──────────────────────────────────────────────────────────────

def load_features(conn: snowflake.connector.SnowflakeConnection) -> pd.DataFrame:
    log.info("Loading %s ...", FEATURE_TABLE)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {FEATURE_TABLE} ORDER BY date, ticker")
    cols = [d[0].lower() for d in cur.description]
    df   = pd.DataFrame(cur.fetchall(), columns=cols)
    df["date"] = pd.to_datetime(df["date"])
    log.info("  %s rows × %s columns", f"{len(df):,}", len(df.columns))
    return df


# ── Feature engineering ───────────────────────────────────────────────────────

def add_sector_dummies(df: pd.DataFrame) -> pd.DataFrame:
    dummies = pd.get_dummies(df["sector"], prefix="sector", drop_first=False)
    return pd.concat([df, dummies.astype(float)], axis=1)


def forward_fill_per_ticker(df: pd.DataFrame) -> pd.DataFrame:
    """Forward-fill monthly economic columns within each ticker series."""
    df = df.sort_values(["ticker", "date"]).copy()
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    df[numeric_cols] = (
        df.groupby("ticker")[numeric_cols]
        .transform(lambda s: s.ffill().bfill())
    )
    return df


def get_feature_cols(df: pd.DataFrame) -> list[str]:
    all_cols    = set(df.columns)
    sector_cols = {c for c in all_cols if c.startswith("sector_")}
    base_cols   = all_cols - EXCLUDE_COLS - sector_cols
    return sorted(base_cols) + sorted(sector_cols)


# ── Training & prediction ─────────────────────────────────────────────────────

def train_horizon(
    df: pd.DataFrame,
    horizon_key: str,
    horizon_cfg: dict,
    feature_cols: list[str],
) -> np.ndarray:
    target_col = horizon_cfg["target"]
    split_col  = horizon_cfg["split_col"]

    train_mask = (df[split_col] == "training") & df[target_col].notna()
    X_train = df.loc[train_mask, feature_cols].astype(float)
    y_train = df.loc[train_mask, target_col].astype(float)
    X_all   = df[feature_cols].astype(float)

    log.info(
        "  [%s] train rows=%s  features=%s",
        horizon_key, f"{len(X_train):,}", len(feature_cols),
    )

    # Median imputation — handles monthly NULLs and any other gaps.
    # Wrap in DataFrames so XGBoost tracks feature names and
    # feature_importances_ always aligns with feature_cols.
    imputer     = SimpleImputer(strategy="median", keep_empty_features=True)
    X_train_imp = pd.DataFrame(
        imputer.fit_transform(X_train), columns=feature_cols
    )
    X_all_imp   = pd.DataFrame(
        imputer.transform(X_all), columns=feature_cols
    )

    model = xgb.XGBRegressor(**XGB_PARAMS)
    model.fit(X_train_imp, y_train)

    importances = pd.Series(model.feature_importances_, index=feature_cols)
    top10 = importances.nlargest(10).round(4).to_dict()
    log.info("  [%s] top features: %s", horizon_key, top10)

    return model.predict(X_all_imp)


# ── Write to Snowflake ────────────────────────────────────────────────────────

DDL = f"""
CREATE OR REPLACE TABLE {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE} (
    ticker           VARCHAR NOT NULL,
    date             DATE    NOT NULL,
    horizon          VARCHAR NOT NULL,
    predicted_return FLOAT,
    dataset_split    VARCHAR,
    model_version    VARCHAR NOT NULL,
    run_timestamp    VARCHAR NOT NULL
)
"""


def ensure_table(conn: snowflake.connector.SnowflakeConnection) -> None:
    conn.cursor().execute(DDL)
    log.info("Table %s ready.", TARGET_TABLE)


def write_predictions(
    conn: snowflake.connector.SnowflakeConnection,
    df: pd.DataFrame,
    model_version: str,
    run_timestamp: datetime,
) -> None:
    log.info("Building predictions DataFrame ...")
    rows = []
    for horizon, cfg in HORIZONS.items():
        split_col = cfg["split_col"]
        pred_col  = f"pred_{horizon}"
        for _, row in df[["ticker", "date", pred_col, split_col]].iterrows():
            rows.append({
                "TICKER":           row["ticker"],
                "DATE":             row["date"].date(),
                "HORIZON":          horizon,
                "PREDICTED_RETURN": float(row[pred_col]),
                "DATASET_SPLIT":    row[split_col],
                "MODEL_VERSION":    model_version,
                "RUN_TIMESTAMP":    run_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            })

    pred_df = pd.DataFrame(rows)
    log.info("  %s prediction rows to write", f"{len(pred_df):,}")

    # Idempotent: delete any prior run for today before re-inserting
    conn.cursor().execute(
        f"DELETE FROM {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE} "
        "WHERE model_version = %s",
        (model_version,),
    )

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=pred_df,
        table_name=TARGET_TABLE,
        database=TARGET_DATABASE,
        schema=TARGET_SCHEMA,
        quote_identifiers=False,
        overwrite=False,
    )
    if not success:
        raise RuntimeError(f"write_pandas failed after {nchunks} chunks / {nrows} rows")
    log.info("  Wrote %s rows in %s chunks.", f"{nrows:,}", nchunks)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    model_version = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_timestamp = datetime.now(timezone.utc)
    log.info("model_version=%s", model_version)

    conn = get_connection()

    ensure_table(conn)

    df = load_features(conn)
    df = forward_fill_per_ticker(df)
    df = add_sector_dummies(df)

    feature_cols = get_feature_cols(df)
    log.info("Feature matrix: %s columns", len(feature_cols))

    for horizon_key, horizon_cfg in HORIZONS.items():
        log.info("Training %s model ...", horizon_key)
        preds = train_horizon(df, horizon_key, horizon_cfg, feature_cols)
        df[f"pred_{horizon_key}"] = preds

    write_predictions(conn, df, model_version, run_timestamp)
    conn.close()
    log.info("Done — model_version=%s", model_version)


if __name__ == "__main__":
    main()
