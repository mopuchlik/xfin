### runner
uv run python -m fin_forecast \
  --folder data/raw \
  --pattern "**/*.mst" \
  --out data/processed/bars \
  --log-level=DEBUG \
  --log-file logs/fin_forecast.log

### tests
uv run pytest
uv run pytest -v

### log levels
DEBUG	10	Detailed internal info, counters, progress
INFO	20	High-level progress, milestones
WARNING	30	Something odd, but program continues
ERROR	40	Operation failed, but program handled it
CRITICAL	50	Program cannot continue





