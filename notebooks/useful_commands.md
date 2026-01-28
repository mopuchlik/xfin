### uv synchro after clone
uv sync 
uv sync --group dev   


### runner for module
uv run python -m xfin.data_engine \
  --folder data/raw \
  --pattern "**/*.mst" \
  --out data/processed/bars \
  --log-level=DEBUG \
  --log-file logs/xfin.data_engine.log

uv run xfin-data --help
uv run xfin-forecast --help

### runner for script
uv run python scripts/load_bars.py

#### for modelling scripts
python scripts/load_bars.py \
  --root data/processed/bars \
  --date-min 2025-01-01 \
  --date-max 2025-12-31 \
  --columns ticker,dt,open,high,low,close,vol \
  --out-csv data/csv/stacked_bars_2025.csv

python scripts/load_bars.py \
  --root data/processed/bars \
  --date-min 2024-01-01 \
  --date-max 2025-12-31 \
  --columns ticker,dt,open,high,low,close,vol \
  --out-csv data/csv/stacked_bars_2024.csv

python scripts/load_bars.py \
  --root data/processed/bars \
  --date-min 2025-12-01 \
  --date-max 2025-12-31 \
  --columns ticker,dt,open,high,low,close,vol \
  --out-csv data/csv/stacked_bars_12.2025.csv

### tests
uv run pytest
uv run pytest -v

### log levels
DEBUG	10	Detailed internal info, counters, progress
INFO	20	High-level progress, milestones
WARNING	30	Something odd, but program continues
ERROR	40	Operation failed, but program handled it
CRITICAL	50	Program cannot continue




### tree
tree -I "*.parquet|*.mst|*.csv|*.pyc"
