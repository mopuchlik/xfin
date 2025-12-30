import pandas as pd

from xfin.forecaster.runner import RunConfig, run_forecast


def _toy_df() -> pd.DataFrame:
    """
    Small panel dataset with two tickers and 3 dates each.
    Used by runner tests to keep expectations simple and deterministic.
    """
    return pd.DataFrame(
        {
            "ticker": ["AAA", "AAA", "AAA", "BBB", "BBB", "BBB"],
            "dt": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03"] * 2
            ),
            "close": [10.0, 11.0, 12.0, 100.0, 101.0, 102.0],
        }
    )


def test_run_forecast_global_mode_returns_single_result():
    """
    In 'global' mode we train/predict using one model on all selected rows.

    Contract:
    - run_forecast(..., mode="global") returns a single ForecastResult
    - Predictions length equals number of supervised rows (after shift/dropna)
    """
    df = _toy_df()
    cfg = RunConfig(model="naive_last_close", mode="global", horizon=1)

    result = run_forecast(df, cfg)

    # global mode => ForecastResult, not a dict
    assert not isinstance(result, dict)

    # horizon=1 removes last row per ticker => 2 tickers * (3-1) = 4 rows
    assert len(result.y_pred) == 4


def test_run_forecast_per_ticker_mode_returns_dict():
    """
    In 'per_ticker' mode we train/predict separate model instances per ticker.

    Contract:
    - run_forecast(..., mode="per_ticker") returns dict[ticker, ForecastResult]
    - Each ticker's prediction length matches that ticker's supervised rows
    """
    df = _toy_df()
    cfg = RunConfig(model="naive_last_close", mode="per_ticker", horizon=1)

    result = run_forecast(df, cfg)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"AAA", "BBB"}
    assert len(result["AAA"].y_pred) == 2
    assert len(result["BBB"].y_pred) == 2


def test_run_forecast_can_limit_to_specific_tickers():
    """
    Runner should support evaluating on a subset of tickers.

    Contract:
    - tickers=["AAA"] means only AAA is included in the run
    """
    df = _toy_df()
    cfg = RunConfig(model="naive_last_close", mode="per_ticker", tickers=["AAA"], horizon=1)

    result = run_forecast(df, cfg)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"AAA"}
    assert len(result["AAA"].y_pred) == 2
