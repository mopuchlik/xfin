import pandas as pd

from xfin.forecaster.dataset import prepare_supervised, select_tickers


def test_select_tickers_filters_when_list_is_provided():
    """
    select_tickers() is a convenience filter used by the runner/CLI.

    Contract:
    - If tickers list is provided, keep only rows for those tickers.
    - Returned DataFrame is a copy (safe to modify downstream).
    """
    df = pd.DataFrame(
        {
            "ticker": ["AAA", "BBB", "AAA"],
            "dt": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02"]),
            "close": [10.0, 20.0, 11.0],
        }
    )
    out = select_tickers(df, ["AAA"])
    assert set(out["ticker"].unique()) == {"AAA"}
    assert len(out) == 2


def test_select_tickers_returns_input_when_none_or_empty():
    """
    Contract:
    - If tickers=None or tickers=[] -> no filtering is applied.
    """
    df = pd.DataFrame(
        {
            "ticker": ["AAA", "BBB"],
            "dt": pd.to_datetime(["2024-01-01", "2024-01-01"]),
            "close": [10.0, 20.0],
        }
    )
    out_none = select_tickers(df, None)
    out_empty = select_tickers(df, [])

    # same content; we don't require same object identity
    assert len(out_none) == len(df)
    assert len(out_empty) == len(df)


def test_prepare_supervised_adds_future_target_per_ticker():
    """
    prepare_supervised() builds a supervised learning target for time-series:

    Contract:
    - Sort by (ticker, dt)
    - Create y = target_col shifted by -horizon within each ticker
    - Drop rows where y is missing (the last `horizon` rows per ticker)
    """
    df = pd.DataFrame(
        {
            "ticker": ["AAA", "AAA", "AAA"],
            "dt": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "close": [10.0, 11.0, 12.0],
        }
    )
    sup = prepare_supervised(df, target_col="close", horizon=1)

    assert "y" in sup.columns
    assert sup["y"].tolist() == [11.0, 12.0]
    assert len(sup) == 2


def test_prepare_supervised_validates_required_columns():
    """
    Contract:
    - prepare_supervised() must fail with a clear error if required columns
      (ticker, dt, target_col) are missing.
    """
    df = pd.DataFrame({"ticker": ["AAA"], "close": [10.0]})  # missing dt
    try:
        prepare_supervised(df, target_col="close", horizon=1)
        assert False, "Expected ValueError due to missing required columns"
    except ValueError as e:
        assert "Missing required columns" in str(e)
