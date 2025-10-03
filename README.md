Capital One Airline Data Challenge — Q1 2019 Working Data Product

Overview
This repo implements a reproducible, code-first analysis targeting the Q1 2019 airline problem statement. It follows a Goal–Actions–Mechanism–Environment (G-A-M-E) blueprint and ships a “working data product” with typed ingestion, quality validation, engineered features, analyses, and visuals.

Scope & assumptions
- Data scope: YEAR=2019, QUARTER=1 only. Flights exclude cancelled legs where CANCELLED==1.
- Airports reference: `Airport_Codes.csv` (IATA 3-letter codes). IATA pattern: ^[A-Z]{3}$.
- Tickets scope: filter to Q1 2019; revenue computed as PASSENGERS × ITIN_FARE for ROUNDTRIP==1.
- Costs: If explicit operating cost is missing, a placeholder assumption will be documented in the notebook. Aircraft purchase cost is explicitly excluded from operating profit.
- Metadata: `Airline_Challenge_Metadata.xlsx` is the schema source of truth used to set types.

Project structure
```
data/
  raw/                # input CSVs live here (copied from data/)
  processed/          # typed/clean outputs (Parquet/CSV)
docs/                 # data dictionary, issue logs
notebooks/            # main orchestrating notebook
src/
  ingest.py           # dataset-specific typed ingestion utilities
  quality.py          # validations and issue logging
  features.py         # route ids, on-time flags, KPIs
README.md
requirements.txt
```

Environment
- Python 3.x
- pandas, numpy, pyarrow, fastparquet, plotly, seaborn, duckdb (optional), python-dotenv

Quickstart
1) Install
```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
2) Data
- Ensure the provided CSVs are available under `data/raw/`:
  - `Airport_Codes.csv`
  - `Flights.csv`
  - `Tickets.csv`
  - `Airline_Challenge_Metadata.xlsx` (schema reference)

3) Run notebook
- Open `notebooks/airline_q12019_analysis.ipynb` in Cursor and run all cells.

Outputs
- Processed tables in `data/processed/` with typed columns
- Issue log and data dictionary in `docs/`
- Ranked tables and plots embedded in the notebook

Key KPIs
- On‑time arrival rate
- Completion factor (share of legs not cancelled)
- Occupancy (load factor)
- Operating margin (ex-aircraft purchase)

License
Open-source for challenge evaluation; see repository license.

# Instructions for Candidates:
`data.zip` contains the data and metadata files necessary to complete the Data Analyst Airline Challenge. Please do not include this data with your submission.
