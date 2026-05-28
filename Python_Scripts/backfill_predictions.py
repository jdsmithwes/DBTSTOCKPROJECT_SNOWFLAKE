#!/usr/bin/env python3
"""
backfill_predictions.py

Walk-forward backtest: for each quarterly cutoff date, trains 4 XGBoost models on
data strictly available at that cutoff (no look-ahead), then writes predictions for
the ~65-trading-day inference window immediately before the cutoff to
RAW_ML_PREDICTIONS with model_version = "backtest_YYYY-MM-DD".

Since all cutoffs are historical, every prediction will have its actual forward
return materialized in mart_ml_features today. After running this script, execute:

    dbt run --select mart_ml_predictions mart_model_evaluation \\
        --profiles-dir . --target dev

to populate mart_model_evaluation with real out-of-sample metrics.

Run:
    cd DBTSTOCKPROJECT_SNOWFLAKE
    set -a && source DBTSTOCKPROJECT/.env && set +a
    python3 Python_Scripts/backfill_predictions.py
"""

import logging
import os
import sys
import warnings
from datetime import datetime, timezone

import pandas as pd
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

# Re-use shared config and helpers from the nightly training script.
sys.path.insert(0, os.path.dirname(__file__))
from train_ml_model import (
    HORIZONS, XGB_PARAMS,
    TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE,
    get_connection, load_features, add_sector_dummies,
    forward_fill_per_ticker, get_feature_cols,
)


# ── Backtest configuration ─────────────────────────────────────────────────────

# Quarterly cutoffs going back ~14 months. At each cutoff the model is trained on
# data available at that date; predictions are made for the ~65-bday window that
# was "inference" at the time.  All 4 horizon targets are fully materialized today
# (2026-05-28) for every cutoff listed here.
BACKTEST_CUTOFFS = [
    "2024-03-29",   # 14 months ago → 12m return matured 2025-03-29  ✓
    "2024-06-28",   # 11 months ago → 12m return matured 2025-06-28  ✓
    "2024-09-30",   # 8 months ago  → 12m return matured 2025-09-30  ✓
    "2024-12-31",   # 5 months ago  → 12m return matured 2025-12-31  ✓
    "2025-03-31",   # 2 months ago  → 12m return matured 2026-03-31  ✓
]

# Approximate trading-day lag before the forward return for date D is
# observable at the cutoff. Used to build the no-look-ahead training mask.
HORIZON_BDAYS = {"3m": 65, "6m": 130, "9m": 195, "12m": 260}

# Inference window width (trading days) — mirrors the mart_ml_features definition.
INFERENCE_WINDOW_BDAYS = 65


# ── Snowflake helpers ──────────────────────────────────────────────────────────

_DDL_IF_NOT_EXISTS = f"""
CREATE TABLE IF NOT EXISTS {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE} (
    ticker           VARCHAR NOT NULL,
    date             DATE    NOT NULL,
    horizon          VARCHAR NOT NULL,
    predicted_return FLOAT,
    dataset_split    VARCHAR,
    model_version    VARCHAR NOT NULL,
    run_timestamp    VARCHAR NOT NULL
)
"""


def ensure_table(conn) -> None:
    """Create RAW_ML_PREDICTIONS if it doesn't already exist (preserves live data)."""
    conn.cursor().execute(_DDL_IF_NOT_EXISTS)
    log.info("Table %s ready.", TARGET_TABLE)


def delete_version(conn, model_version: str) -> None:
    conn.cursor().execute(
        f"DELETE FROM {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE} "
        "WHERE model_version = %s",
        (model_version,),
    )


# ── Backtest core ──────────────────────────────────────────────────────────────

def run_backtest_cutoff(
    df: pd.DataFrame,
    feature_cols: list[str],
    cutoff_date: str,
    run_timestamp: datetime,
    conn,
) -> None:
    cutoff_dt     = pd.Timestamp(cutoff_date)
    model_version = f"backtest_{cutoff_date}"
    log.info("━━━ Cutoff: %s  (model_version=%s) ━━━", cutoff_date, model_version)

    # Inference window: the ~65 bdays leading up to the cutoff.
    # These rows were "inference" at the time — no known forward return yet.
    # Today they have materialized actual returns, making honest evaluation possible.
    inf_start = cutoff_dt - pd.offsets.BDay(INFERENCE_WINDOW_BDAYS)
    inf_mask  = (df["date"] > inf_start) & (df["date"] <= cutoff_dt)
    df_inf    = df[inf_mask].reset_index(drop=True)

    if df_inf.empty:
        log.warning("No inference rows for cutoff %s — skipping.", cutoff_date)
        return

    log.info(
        "  Inference window: %s → %s  (%s rows, %s tickers)",
        df_inf["date"].min().date(), df_inf["date"].max().date(),
        f"{len(df_inf):,}", df_inf["ticker"].nunique(),
    )

    # Idempotent: remove any prior run for this backtest version.
    delete_version(conn, model_version)

    chunks = []
    for horizon, cfg in HORIZONS.items():
        target_col = cfg["target"]

        # No-look-ahead training set: only rows whose forward return was
        # observable at the cutoff (i.e. date + horizon_lag <= cutoff_date).
        cutoff_minus_lag = cutoff_dt - pd.offsets.BDay(HORIZON_BDAYS[horizon])
        train_mask       = (df["date"] <= cutoff_minus_lag) & df[target_col].notna()
        X_train = df.loc[train_mask, feature_cols].astype(float)
        y_train = df.loc[train_mask, target_col].astype(float)

        log.info(
            "  [%s] training cutoff=%s  rows=%s",
            horizon, cutoff_minus_lag.date(), f"{len(X_train):,}",
        )

        # Fit imputer on training data; transform both training and inference.
        imputer     = SimpleImputer(strategy="median", keep_empty_features=True)
        X_train_imp = pd.DataFrame(imputer.fit_transform(X_train), columns=feature_cols)
        X_inf_imp   = pd.DataFrame(
            imputer.transform(df_inf[feature_cols].astype(float)),
            columns=feature_cols,
        )

        model = xgb.XGBRegressor(**XGB_PARAMS)
        model.fit(X_train_imp, y_train)

        preds = model.predict(X_inf_imp)

        importances = pd.Series(model.feature_importances_, index=feature_cols)
        log.info("  [%s] top-5: %s", horizon,
                 importances.nlargest(5).round(4).to_dict())

        chunk = pd.DataFrame({
            "TICKER":           df_inf["ticker"].values,
            "DATE":             [d.date() for d in df_inf["date"]],
            "HORIZON":          horizon,
            "PREDICTED_RETURN": preds.astype(float),
            "DATASET_SPLIT":    "backtest",   # clearly marks out-of-sample rows
            "MODEL_VERSION":    model_version,
            "RUN_TIMESTAMP":    run_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        })
        chunks.append(chunk)

    pred_df = pd.concat(chunks, ignore_index=True)
    log.info("  Writing %s prediction rows ...", f"{len(pred_df):,}")

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
        raise RuntimeError(
            f"write_pandas failed for {model_version}: {nchunks} chunks / {nrows} rows"
        )
    log.info("  ✓ Wrote %s rows.", f"{nrows:,}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    run_timestamp = datetime.now(timezone.utc)
    log.info(
        "Starting backfill — %s quarterly cutoffs: %s",
        len(BACKTEST_CUTOFFS), BACKTEST_CUTOFFS,
    )

    conn = get_connection()
    ensure_table(conn)

    log.info("Loading features from Snowflake ...")
    df = load_features(conn)
    df = forward_fill_per_ticker(df)
    df = add_sector_dummies(df)
    feature_cols = get_feature_cols(df)
    log.info("Feature matrix: %s rows × %s feature columns", f"{len(df):,}", len(feature_cols))

    for cutoff_date in BACKTEST_CUTOFFS:
        run_backtest_cutoff(df, feature_cols, cutoff_date, run_timestamp, conn)

    conn.close()
    log.info("Backfill complete — %s versions written.", len(BACKTEST_CUTOFFS))
    log.info(
        "Next: dbt run --select mart_ml_predictions mart_model_evaluation "
        "--profiles-dir . --target dev"
    )


if __name__ == "__main__":
    main()
