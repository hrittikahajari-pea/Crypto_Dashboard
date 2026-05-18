import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

from db_connect import get_connection

try:
    from joblib import dump, load
except ImportError:
    dump = None
    load = None

try:
    from sklearn.ensemble import GradientBoostingRegressor, HistGradientBoostingRegressor
    from sklearn.linear_model import HuberRegressor, Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import RobustScaler
except ImportError:
    GradientBoostingRegressor = None
    HistGradientBoostingRegressor = None
    HuberRegressor = None
    Ridge = None
    RobustScaler = None
    make_pipeline = None


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

FORECAST_HORIZON = "Next 30-minute candle"
DEFAULT_MODEL = "auto"
SUPPORTED_MODELS = {"auto", "fallback", "advanced"}

MIN_FALLBACK_ROWS = 20
MIN_ADVANCED_ROWS = 35
MIN_ENGINEERED_ROWS = 18
RECENT_VALIDATION_ROWS = 20
MIN_ACCEPTABLE_ADVANCED_SIGNAL = 15.0

ARTIFACT_DIR = Path(__file__).resolve().parent / "model_artifacts"


def _safe_float(value, fallback=0.0):
    """Return a finite float, using fallback for missing or invalid values."""
    if value is None:
        return fallback

    try:
        value = float(value)
    except (TypeError, ValueError):
        return fallback

    return value if np.isfinite(value) else fallback


def _optional_float(value):
    """Return a finite float or None for optional metrics."""
    if value is None:
        return None

    try:
        value = float(value)
    except (TypeError, ValueError):
        return None

    return value if np.isfinite(value) else None


def _clamp(value, lower=0.0, upper=100.0):
    return max(lower, min(upper, _safe_float(value)))


def _slugify(value):
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_") or "coin"


def _artifact_path(coin_name):
    return ARTIFACT_DIR / f"{_slugify(coin_name)}_forecast.joblib"


def _fallback_artifact_path(coin_name):
    return ARTIFACT_DIR / f"{_slugify(coin_name)}_fallback_strategy.joblib"


def load_coin_history(coin_name: str) -> pd.DataFrame:
    """Load all usable historical rows for a coin in chronological order."""
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                market_timestamp,
                price,
                volume,
                market_cap,
                ingested_at
            FROM crypto_prices
            WHERE LOWER(coin_name) = LOWER(%s)
              AND market_timestamp IS NOT NULL
              AND price IS NOT NULL
            ORDER BY market_timestamp ASC;
            """,
            (coin_name,),
        )

        rows = cur.fetchall()
        logger.info("Rows loaded for %s: %s", coin_name, len(rows))

        df = pd.DataFrame(
            rows,
            columns=[
                "market_timestamp",
                "price",
                "volume",
                "market_cap",
                "ingested_at",
            ],
        )

        if df.empty:
            return df

        df["market_timestamp"] = pd.to_datetime(
            df["market_timestamp"],
            errors="coerce",
        )

        for column in ["price", "volume", "market_cap"]:
            df[column] = pd.to_numeric(df[column], errors="coerce")

        df = df.dropna(subset=["market_timestamp", "price"])
        df = df.sort_values("market_timestamp").reset_index(drop=True)

        return df

    except Exception as error:
        logger.exception("Failed to load history for %s: %s", coin_name, error)
        raise

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def get_signal_label(score: float) -> str:
    if score >= 85:
        return "Excellent"
    if score >= 70:
        return "Strong"
    if score >= 50:
        return "Good"
    if score >= 30:
        return "Moderate"
    if score >= 10:
        return "Weak"

    return "Poor"


def get_trend(change_percent: float) -> str:
    if change_percent > 0.2:
        return "Bullish"
    if change_percent < -0.2:
        return "Bearish"

    return "Neutral"


def calculate_metrics(current_prices, actual_prices, predicted_prices):
    """Calculate chronological one-step forecast quality metrics."""
    current_prices = np.asarray(current_prices, dtype=float)
    actual_prices = np.asarray(actual_prices, dtype=float)
    predicted_prices = np.asarray(predicted_prices, dtype=float)

    if len(actual_prices) == 0:
        return None, None, None, None

    errors = predicted_prices - actual_prices
    mae = float(np.mean(np.abs(errors)))
    rmse = float(np.sqrt(np.mean(np.square(errors))))

    predicted_direction = np.sign(predicted_prices - current_prices)
    actual_direction = np.sign(actual_prices - current_prices)
    directional_accuracy = float((predicted_direction == actual_direction).mean() * 100)

    average_price = float(np.mean(np.abs(actual_prices)))
    relative_rmse = float(rmse / average_price) if average_price else None

    return mae, rmse, directional_accuracy, relative_rmse


def signal_from_validation(directional_accuracy, relative_rmse, predicted_change_percent):
    """Score signal strength from observed validation performance.

    Directional accuracy is the main component. Error and overlarge projected
    moves reduce the score, so the value is not a fake model confidence.
    """
    if directional_accuracy is None:
        return 0.0

    direction_component = _safe_float(directional_accuracy) * 0.82
    edge_component = max(0.0, _safe_float(directional_accuracy) - 50.0) * 0.18
    move_component = min(abs(_safe_float(predicted_change_percent)) * 2.5, 6.0)
    error_penalty = min(_safe_float(relative_rmse) * 900.0, 18.0)

    return _clamp(direction_component + edge_component + move_component - error_penalty)


def build_response(
    coin_name,
    model_name,
    model_type,
    current_price,
    predicted_price,
    predicted_change_percent,
    signal_strength_score,
    data_points_used,
    note,
    mae=None,
    rmse=None,
    directional_accuracy=None,
):
    """Build the stable schema consumed by the FastAPI route and React UI."""
    current_price = _safe_float(current_price)
    predicted_price = _safe_float(predicted_price, current_price)
    predicted_change_percent = _safe_float(predicted_change_percent)
    signal_strength_score = _clamp(signal_strength_score)

    response = {
        "coin_name": coin_name,
        "forecast_horizon": FORECAST_HORIZON,
        "model_name": model_name,
        "model_type": model_type,
        "current_price": current_price,
        "predicted_price": predicted_price,
        "predicted_change_percent": predicted_change_percent,
        "predicted_trend": get_trend(predicted_change_percent),
        "signal_strength_score": signal_strength_score,
        "signal_label": get_signal_label(signal_strength_score),
        "data_points_used": int(data_points_used),
        "mae": _optional_float(mae),
        "rmse": _optional_float(rmse),
        "directional_accuracy": _optional_float(directional_accuracy),
        "note": note,
    }

    logger.info(
        "%s forecast for %s: trend=%s signal=%.2f dir_acc=%s mae=%s rmse=%s",
        model_type,
        coin_name,
        response["predicted_trend"],
        signal_strength_score,
        response["directional_accuracy"],
        response["mae"],
        response["rmse"],
    )

    return response


def _momentum_next_price(prices: pd.Series):
    """Predict the next price with dampened multi-window momentum."""
    current_price = _safe_float(prices.iloc[-1])
    returns = prices.pct_change().replace([np.inf, -np.inf], np.nan).dropna()

    short_momentum = _safe_float(returns.tail(3).mean())
    medium_momentum = _safe_float(returns.tail(12).mean())
    long_momentum = _safe_float(returns.tail(24).mean())
    recent_volatility = abs(_safe_float(returns.tail(12).std()))

    combined_momentum = (
        0.55 * short_momentum
        + 0.30 * medium_momentum
        + 0.15 * long_momentum
    )
    dampening = max(0.15, 1.0 - min(recent_volatility * 12.0, 0.85))

    return _safe_float(current_price * (1.0 + combined_momentum * dampening), current_price)


def _strategy_next_price(prices: pd.Series, strategy: str, invert=False):
    """Predict next price with a validated lightweight strategy.

    Crypto often flips between momentum and mean reversion at short horizons.
    We backtest both families and only use the contrarian inversion when recent
    validation shows the raw strategy is consistently wrong.
    """
    current_price = _safe_float(prices.iloc[-1])
    returns = prices.pct_change().replace([np.inf, -np.inf], np.nan).dropna()

    if strategy == "momentum":
        predicted_price = _momentum_next_price(prices)
    elif strategy == "short_momentum":
        recent_return = _safe_float(returns.tail(3).mean())
        predicted_price = current_price * (1.0 + recent_return * 0.65)
    elif strategy == "mean_reversion":
        moving_average = _safe_float(prices.tail(10).mean(), current_price)
        reversion_return = ((moving_average - current_price) / current_price) * 0.35
        predicted_price = current_price * (1.0 + reversion_return)
    elif strategy == "volatility_reversion":
        moving_average = _safe_float(prices.tail(6).mean(), current_price)
        volatility = abs(_safe_float(returns.tail(10).std()))
        reversion_weight = max(0.15, min(0.55, volatility * 18.0))
        reversion_return = ((moving_average - current_price) / current_price) * reversion_weight
        predicted_price = current_price * (1.0 + reversion_return)
    else:
        predicted_price = current_price

    if invert:
        predicted_price = current_price - (predicted_price - current_price)

    return _safe_float(predicted_price, current_price)


def _score_strategy(df: pd.DataFrame, strategy: str, invert=False):
    """Backtest one lightweight strategy on recent chronological rows."""
    if len(df) < MIN_FALLBACK_ROWS + 2:
        return None

    start_index = max(MIN_FALLBACK_ROWS, len(df) - RECENT_VALIDATION_ROWS)
    current_prices = []
    actual_prices = []
    predicted_prices = []

    for index in range(start_index, len(df)):
        history = df["price"].iloc[:index]
        current_prices.append(_safe_float(history.iloc[-1]))
        actual_prices.append(_safe_float(df["price"].iloc[index]))
        predicted_prices.append(_strategy_next_price(history, strategy, invert=invert))

    mae, rmse, directional_accuracy, relative_rmse = calculate_metrics(
        current_prices,
        actual_prices,
        predicted_prices,
    )
    signal_strength_score = signal_from_validation(directional_accuracy, relative_rmse, 0.0)

    return {
        "strategy": strategy,
        "invert": invert,
        "mae": mae,
        "rmse": rmse,
        "directional_accuracy": directional_accuracy,
        "relative_rmse": relative_rmse,
        "signal_strength_score": signal_strength_score,
    }


def select_fallback_strategy(df: pd.DataFrame):
    """Select the best recent fallback strategy using real backtest results."""
    strategies = [
        "momentum",
        "short_momentum",
        "mean_reversion",
        "volatility_reversion",
    ]
    results = []

    for strategy in strategies:
        for invert in (False, True):
            result = _score_strategy(df, strategy, invert=invert)
            if result is not None:
                results.append(result)

    if not results:
        return None

    best_result = max(results, key=lambda item: item["signal_strength_score"])
    logger.info(
        "Selected fallback strategy=%s invert=%s signal=%.2f dir_acc=%s",
        best_result["strategy"],
        best_result["invert"],
        best_result["signal_strength_score"],
        best_result["directional_accuracy"],
    )

    return best_result


def generate_fallback_forecast(coin_name: str, df: pd.DataFrame):
    """Generate a simple, robust forecast that is always tried as backup."""
    if df is None or len(df) < MIN_FALLBACK_ROWS:
        logger.warning(
            "Fallback unavailable for %s. Required rows: %s, found: %s.",
            coin_name,
            MIN_FALLBACK_ROWS,
            0 if df is None else len(df),
        )
        return None

    selected_strategy = select_fallback_strategy(df)
    strategy_name = "momentum"
    invert_strategy = False

    if selected_strategy is not None:
        strategy_name = selected_strategy["strategy"]
        invert_strategy = selected_strategy["invert"]
        save_fallback_artifact(
            coin_name,
            {
                "strategy": strategy_name,
                "invert": invert_strategy,
                "metrics": {
                    "mae": selected_strategy.get("mae"),
                    "rmse": selected_strategy.get("rmse"),
                    "directional_accuracy": selected_strategy.get(
                        "directional_accuracy"
                    ),
                    "relative_rmse": selected_strategy.get("relative_rmse"),
                    "signal_strength_score": selected_strategy.get(
                        "signal_strength_score"
                    ),
                },
                "row_count": len(df),
                "latest_timestamp": str(df["market_timestamp"].iloc[-1]),
            },
        )

    current_price = _safe_float(df["price"].iloc[-1])
    predicted_price = _strategy_next_price(
        df["price"],
        strategy_name,
        invert=invert_strategy,
    )
    predicted_change_percent = (
        ((predicted_price - current_price) / current_price) * 100.0
        if current_price
        else 0.0
    )

    mae = selected_strategy.get("mae") if selected_strategy else None
    rmse = selected_strategy.get("rmse") if selected_strategy else None
    directional_accuracy = (
        selected_strategy.get("directional_accuracy") if selected_strategy else None
    )
    relative_rmse = selected_strategy.get("relative_rmse") if selected_strategy else None
    signal_strength_score = signal_from_validation(
        directional_accuracy,
        relative_rmse,
        predicted_change_percent,
    )

    return build_response(
        coin_name=coin_name,
        model_name="Adaptive Momentum Fallback",
        model_type="fallback",
        current_price=current_price,
        predicted_price=predicted_price,
        predicted_change_percent=predicted_change_percent,
        signal_strength_score=signal_strength_score,
        data_points_used=len(df),
        mae=mae,
        rmse=rmse,
        directional_accuracy=directional_accuracy,
        note=(
            f"Fallback selected {strategy_name}"
            f"{' with validated contrarian inversion' if invert_strategy else ''}. "
            "Signal strength is based on recent chronological directional accuracy "
            "and relative forecast error."
        ),
    )


def create_features(df: pd.DataFrame):
    """Create short-horizon return features.

    For this project, a tabular return model is the most practical stronger
    approach: the dataset is small, updated frequently, and needs quick retrains
    without a heavy sequence-model serving stack.
    """
    feature_df = df.copy()
    feature_columns = []
    returns = feature_df["price"].pct_change()

    for lag in range(1, 13):
        column = f"return_lag_{lag}"
        feature_df[column] = returns.shift(lag)
        feature_columns.append(column)

    engineered = {
        "return_1": returns,
        "return_3": feature_df["price"].pct_change(periods=3),
        "return_6": feature_df["price"].pct_change(periods=6),
        "ma_ratio_6": feature_df["price"] / feature_df["price"].rolling(6).mean() - 1,
        "ma_ratio_12": feature_df["price"] / feature_df["price"].rolling(12).mean() - 1,
        "volatility_6": returns.rolling(6).std(),
        "volatility_12": returns.rolling(12).std(),
        "range_proxy": (
            feature_df["price"].rolling(6).max()
            / feature_df["price"].rolling(6).min()
            - 1
        ),
    }

    for column, values in engineered.items():
        feature_df[column] = values
        feature_columns.append(column)

    if "volume" in feature_df and feature_df["volume"].notna().any():
        feature_df["volume_change"] = feature_df["volume"].pct_change()
        feature_df["volume_z"] = (
            (feature_df["volume"] - feature_df["volume"].rolling(12).mean())
            / feature_df["volume"].rolling(12).std()
        )
        feature_columns.extend(["volume_change", "volume_z"])

    if "market_cap" in feature_df and feature_df["market_cap"].notna().any():
        feature_df["market_cap_change"] = feature_df["market_cap"].pct_change()
        feature_columns.append("market_cap_change")

    feature_df["target_return"] = feature_df["price"].pct_change().shift(-1)
    feature_df = feature_df.replace([np.inf, -np.inf], np.nan)
    feature_df = feature_df.dropna(subset=feature_columns).reset_index(drop=True)

    return feature_df, feature_columns


def make_candidate_models():
    """Create available advanced candidates without optional hard failures."""
    candidates = []

    if make_pipeline is not None and RobustScaler is not None and Ridge is not None:
        candidates.append(
            (
                "regularized_return_regression",
                "Regularized Return Regression",
                make_pipeline(RobustScaler(), Ridge(alpha=1.5)),
            )
        )

    if make_pipeline is not None and RobustScaler is not None and HuberRegressor is not None:
        candidates.append(
            (
                "robust_return_regression",
                "Robust Return Regression",
                make_pipeline(RobustScaler(), HuberRegressor(alpha=0.0005, max_iter=300)),
            )
        )

    if GradientBoostingRegressor is not None:
        candidates.append(
            (
                "gradient_boosted_returns",
                "Gradient Boosted Return Forecaster",
                GradientBoostingRegressor(
                    n_estimators=160,
                    learning_rate=0.035,
                    max_depth=2,
                    min_samples_leaf=4,
                    subsample=0.85,
                    random_state=42,
                ),
            )
        )

    if HistGradientBoostingRegressor is not None:
        candidates.append(
            (
                "hist_gradient_boosted_returns",
                "Histogram Gradient Boosted Return Forecaster",
                HistGradientBoostingRegressor(
                    max_iter=140,
                    learning_rate=0.04,
                    max_leaf_nodes=12,
                    l2_regularization=0.1,
                    random_state=42,
                ),
            )
        )

    return candidates


def validate_candidate(candidate, training_df, feature_columns):
    """Train on older rows and validate only on the latest chronological rows."""
    model_type, model_name, estimator = candidate
    validation_size = min(RECENT_VALIDATION_ROWS, max(8, len(training_df) // 4))
    split_index = len(training_df) - validation_size

    train_df = training_df.iloc[:split_index]
    test_df = training_df.iloc[split_index:]

    estimator.fit(train_df[feature_columns], train_df["target_return"])
    predicted_returns = estimator.predict(test_df[feature_columns])

    current_prices = test_df["price"].to_numpy(dtype=float)
    predicted_prices = current_prices * (1.0 + predicted_returns)
    actual_prices = current_prices * (
        1.0 + test_df["target_return"].to_numpy(dtype=float)
    )

    mae, rmse, directional_accuracy, relative_rmse = calculate_metrics(
        current_prices,
        actual_prices,
        predicted_prices,
    )
    validation_signal = signal_from_validation(directional_accuracy, relative_rmse, 0.0)

    logger.info(
        "Candidate %s validated: signal=%.2f dir_acc=%s mae=%s rmse=%s",
        model_type,
        validation_signal,
        directional_accuracy,
        mae,
        rmse,
    )

    return {
        "model_type": model_type,
        "model_name": model_name,
        "estimator": estimator,
        "mae": mae,
        "rmse": rmse,
        "directional_accuracy": directional_accuracy,
        "relative_rmse": relative_rmse,
        "validation_signal": validation_signal,
    }


def save_artifact(coin_name, artifact):
    if dump is None:
        logger.info("joblib is unavailable; model artifact will not be saved.")
        return

    try:
        ARTIFACT_DIR.mkdir(exist_ok=True)
        dump(artifact, _artifact_path(coin_name))
    except Exception as error:
        logger.warning("Could not save model artifact for %s: %s", coin_name, error)


def save_fallback_artifact(coin_name, artifact):
    """Persist the selected fallback strategy and its validation metrics."""
    if dump is None:
        return

    try:
        ARTIFACT_DIR.mkdir(exist_ok=True)
        dump(artifact, _fallback_artifact_path(coin_name))
    except Exception as error:
        logger.warning(
            "Could not save fallback strategy artifact for %s: %s",
            coin_name,
            error,
        )


def load_artifact(coin_name):
    if load is None:
        return None

    path = _artifact_path(coin_name)
    if not path.exists():
        return None

    try:
        return load(path)
    except Exception as error:
        logger.warning("Could not load model artifact for %s: %s", coin_name, error)
        return None


def artifact_matches(artifact, df, feature_columns):
    """Reuse artifacts only when data/features match and validation stayed useful."""
    metadata = artifact.get("metadata", {})

    return (
        metadata.get("row_count") == len(df)
        and metadata.get("latest_timestamp") == str(df["market_timestamp"].iloc[-1])
        and artifact.get("feature_columns") == feature_columns
        and _safe_float(metadata.get("validation_signal")) >= MIN_ACCEPTABLE_ADVANCED_SIGNAL
    )


def train_or_load_best_advanced(coin_name, df, training_df, feature_columns):
    artifact = load_artifact(coin_name)
    if artifact is not None and artifact_matches(artifact, df, feature_columns):
        logger.info("Using saved forecast artifact for %s.", coin_name)
        return artifact

    candidates = make_candidate_models()
    if not candidates:
        logger.warning("No advanced forecasting candidates are available.")
        return None

    best_result = None
    for candidate in candidates:
        try:
            result = validate_candidate(candidate, training_df, feature_columns)
        except Exception as error:
            logger.warning("Candidate %s failed validation: %s", candidate[0], error)
            continue

        if best_result is None or result["validation_signal"] > best_result["validation_signal"]:
            best_result = result

    if best_result is None:
        return None

    if best_result["validation_signal"] < MIN_ACCEPTABLE_ADVANCED_SIGNAL:
        logger.warning(
            "Best advanced signal too weak for %s: %.2f.",
            coin_name,
            best_result["validation_signal"],
        )
        return None

    final_model = best_result["estimator"]
    final_model.fit(training_df[feature_columns], training_df["target_return"])

    artifact = {
        "model": final_model,
        "feature_columns": feature_columns,
        "metadata": {
            "row_count": len(df),
            "latest_timestamp": str(df["market_timestamp"].iloc[-1]),
            "model_name": best_result["model_name"],
            "candidate_type": best_result["model_type"],
            "mae": best_result["mae"],
            "rmse": best_result["rmse"],
            "directional_accuracy": best_result["directional_accuracy"],
            "relative_rmse": best_result["relative_rmse"],
            "validation_signal": best_result["validation_signal"],
        },
    }
    save_artifact(coin_name, artifact)

    return artifact


def generate_advanced_forecast(coin_name: str, df: pd.DataFrame):
    """Generate the stronger forecast or return None on weak validation."""
    if df is None or len(df) < MIN_ADVANCED_ROWS:
        logger.warning(
            "Advanced unavailable for %s. Required rows: %s, found: %s.",
            coin_name,
            MIN_ADVANCED_ROWS,
            0 if df is None else len(df),
        )
        return None

    try:
        feature_df, feature_columns = create_features(df)
        training_df = feature_df.dropna(subset=["target_return"]).reset_index(drop=True)

        logger.info(
            "Advanced feature rows for %s: %s; training rows: %s.",
            coin_name,
            len(feature_df),
            len(training_df),
        )

        if len(training_df) < MIN_ENGINEERED_ROWS:
            return None

        artifact = train_or_load_best_advanced(
            coin_name,
            df,
            training_df,
            feature_columns,
        )
        if artifact is None:
            return None

        model = artifact["model"]
        metadata = artifact["metadata"]
        latest_features = feature_df[feature_columns].iloc[[-1]]
        predicted_return = _safe_float(model.predict(latest_features)[0])
        current_price = _safe_float(feature_df["price"].iloc[-1])
        predicted_price = current_price * (1.0 + predicted_return)
        predicted_change_percent = predicted_return * 100.0
        signal_strength_score = signal_from_validation(
            metadata.get("directional_accuracy"),
            metadata.get("relative_rmse"),
            predicted_change_percent,
        )

        return build_response(
            coin_name=coin_name,
            model_name=metadata["model_name"],
            model_type="advanced",
            current_price=current_price,
            predicted_price=predicted_price,
            predicted_change_percent=predicted_change_percent,
            signal_strength_score=signal_strength_score,
            data_points_used=len(df),
            mae=metadata.get("mae"),
            rmse=metadata.get("rmse"),
            directional_accuracy=metadata.get("directional_accuracy"),
            note=(
                f"Advanced forecast selected {metadata['model_name']} from "
                "time-aware candidates. Signal strength is based on recent "
                "chronological directional accuracy and relative forecast error."
            ),
        )

    except Exception as error:
        logger.exception("Advanced forecast failed for %s: %s", coin_name, error)
        return None


def _append_fallback_note(forecast, reason):
    if forecast is None:
        return None

    forecast["note"] = f"{forecast['note']} Fallback reason: {reason}."
    return forecast


def choose_best_forecast(coin_name: str, df: pd.DataFrame):
    """Choose advanced only when recent validation beats the fallback."""
    fallback = generate_fallback_forecast(coin_name, df)
    if fallback is None:
        return None

    advanced = generate_advanced_forecast(coin_name, df)
    if advanced is None:
        return _append_fallback_note(
            fallback,
            "advanced candidates were unavailable or failed recent validation",
        )

    fallback_score = _safe_float(fallback["signal_strength_score"])
    advanced_score = _safe_float(advanced["signal_strength_score"])

    if advanced_score >= fallback_score:
        return advanced

    logger.info(
        "Fallback selected for %s because score beat advanced: fallback=%.2f advanced=%.2f.",
        coin_name,
        fallback_score,
        advanced_score,
    )
    return _append_fallback_note(
        fallback,
        "fallback had stronger recent validation than advanced candidates",
    )


def generate_forecast(coin_name: str, model: str = DEFAULT_MODEL):
    """Generate forecast using auto, fallback, or advanced mode."""
    selected_model = (model or DEFAULT_MODEL).lower()
    logger.info("Selected forecast model for %s: %s", coin_name, selected_model)

    if selected_model not in SUPPORTED_MODELS:
        raise ValueError("Invalid model. Supported values are: auto, fallback, advanced.")

    df = load_coin_history(coin_name)
    logger.info("Forecast rows available for %s: %s", coin_name, len(df))

    if selected_model == "fallback":
        return generate_fallback_forecast(coin_name, df)

    if selected_model == "advanced":
        advanced = generate_advanced_forecast(coin_name, df)
        if advanced is not None:
            return advanced

        return _append_fallback_note(
            generate_fallback_forecast(coin_name, df),
            "advanced model failed, was unavailable, or underperformed validation",
        )

    return choose_best_forecast(coin_name, df)
