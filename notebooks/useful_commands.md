# uv synchro after clone
uv sync
uv sync --group dev   


# runner for data_engine
uv run python -m xfin.data_engine \
  --folder data/raw \
  --pattern "**/*.mst" \
  --out data/processed/bars \
  --log-level=DEBUG \
  --log-file logs/xfin.data_engine.log

uv run xfin-data --help

# runner for forecaster
## all
uv run python -m xfin.forecaster \
  --data data/processed/bars \
  --model naive_last_close \
  --horizon 1 \
  --log-level INFO \
  --log-file logs/xfin.forecaster.log

## for separate tickers
uv run python -m xfin.forecaster \
  --data data/processed/bars \
  --model naive_last_close \
  --mode per_ticker \
  --tickers "WIG20,MBANK,WIG-POLAND" \
  --horizon 5 \
  --log-level DEBUG \
  --log-file logs/xfin.forecaster.log

uv run python -m xfin.forecaster \
  --model xgb_panel \
  --data-parquet data/processed/bars \
  --tickers MBANK WIG20 \
  --date-min 2022-01-01 \
  --date-max 2024-12-31 \
  --artifact data/interim/xgb_panel.joblib

uv run xfin-forecast --help

# runner for script
uv run python scripts/load_bars.py

uv run python scripts/load_bars.py \
  --root data/processed/bars \
  --date-min 2024-01-01 \
  --out data/parquet/bars_stacked.parquet

# tests
uv run pytest
uv run pytest -v

# log levels
DEBUG	10	Detailed internal info, counters, progress
INFO	20	High-level progress, milestones
WARNING	30	Something odd, but program continues
ERROR	40	Operation failed, but program handled it
CRITICAL	50	Program cannot continue

# tree
tree -I "*.parquet|*.mst|*.csv|*.pyc"
