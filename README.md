# openFDA ELT Pipeline

An educational ELT pipeline for adverse drug event reports from the openFDA REST API.

> **This is a fork.** The original is
> [peterscheinsohn/openfda-elt-pipeline](https://github.com/peterscheinsohn/openfda-elt-pipeline)
> and everything below this note is his, unmodified.
>
> The fork adds [`openfda_bruin/`](./openfda_bruin), whose purpose is to show
> **how to rewrite a dlt + dbt project in [Bruin](https://github.com/bruin-data/bruin)**.
> It ports this pipeline asset for asset and works through the "possible future
> improvements" listed at the bottom of this file: incremental loading,
> analytical marts and handling of updated reports.
>
> See [`openfda_bruin/README.md`](./openfda_bruin/README.md) for the lineage,
> a side-by-side on files, lines and setup steps, and the three things the real
> data turned out to need that a five-row sample could not show.

## Architecture

openFDA REST API
→ dlt
→ DuckDB
→ dbt staging models
→ dbt data quality tests

## Tech Stack

- Python 3.13
- dlt
- DuckDB
- dbt Core
- dbt-duckdb
- openFDA REST API

## Setup

Install the required Python packages:

```bash
pip install dlt duckdb dbt-duckdb requests
```

Before running dbt, configure a local DuckDB profile named `openfda_dbt` in:

```text
~/.dbt/profiles.yml
```

Example:

```yaml
openfda_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /path/to/openfda_dlt_rest.duckdb
      schema: main
      threads: 4
```

Replace `/path/to/` with a local path. Do not publish this profile or any credentials.

## Run the Pipeline

Run the dlt pipeline from the project root:

```bash
python3 pipeline_dlt_rest.py
```

The pipeline reads data from:

```text
https://api.fda.gov/drug/event.json
```

and loads raw data into DuckDB.

Build the dbt models and run data quality tests:

```bash
dbt build --project-dir openfda_dbt
```

## dbt Models

- `stg_reports` — report-level data.
- `stg_reactions` — reactions linked to their reports.
- `stg_drugs` — drugs linked to their reports.

## Data Quality

The project checks that:

- report IDs are not null;
- report IDs are unique in `stg_reports`;
- reaction and drug fields are not null;
- list indexes are not null.

Expected result:

```text
3 view models
8 data tests
PASS=11
WARN=0
ERROR=0
```

## Limitations

This is an educational project using a small API sample.

Possible future improvements:

- incremental loading;
- analytical dbt marts;
- dashboard;
- orchestration with Airflow;
- historical tracking of updated reports.

Local DuckDB files, logs, temporary dbt files, profiles, and credentials are not included in the repository.