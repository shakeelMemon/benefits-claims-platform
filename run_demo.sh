#!/usr/bin/env bash
# Two business days end to end, then the business questions. Run from the project folder in WSL/Linux.
set -euo pipefail
rm -rf local
python data_gen/generate_sources.py --day 1
python orchestration/run_local.py --run-date 2026-10-20 --with-stream
python data_gen/generate_sources.py --day 2
python orchestration/run_local.py --run-date 2026-10-21 --with-stream
python sql/query_local.py
