# openFDA adverse drug events, in Bruin

A port of [peterscheinsohn/openfda-elt-pipeline](https://github.com/peterscheinsohn/openfda-elt-pipeline)
from dlt + dbt to [Bruin](https://github.com/bruin-data/bruin). The original is
an educational project and says so; it stops at three staging views over a
five-row sample. Its README lists what it would do next:

> incremental loading; analytical dbt marts; dashboard; orchestration with
> Airflow; historical tracking of updated reports.

This version does the first, second and fifth of those, and Bruin covers the
fourth on its own. The original code is untouched and still sits in
[`../pipeline_dlt_rest.py`](../pipeline_dlt_rest.py) and
[`../openfda_dbt/`](../openfda_dbt) if you want to compare.

Everything below was run against the live API, and every number is from the
run in [`run.log`](./run.log).

## Run it

```bash
curl -LsSf https://getbruin.com/install.sh | sh   # if you do not have it
bruin run . --start-date 2024-02-28 --end-date 2024-03-04
```

That is the whole setup. No profiles file, no virtualenv, no `pip install`, no
separate `dbt build` step. Bruin resolves the Python dependencies with `uv`,
runs the ingestion, materialises the SQL models in dependency order and runs
every quality check. The DuckDB connection is committed in
[`../.bruin.yml`](../.bruin.yml) because a local file path is not a secret;
real projects keep that file out of git.

Then poke at it:

```bash
bruin query -c duckdb-default -q "SELECT * FROM openfda.mart_daily_report_volume ORDER BY 1"
bruin lineage assets/openfda/mart_drug_reaction_signals.sql
bruin validate .
```

## The pipeline

| Asset | Type | Materialisation | Rows |
|---|---|---|---|
| `openfda_raw.drug_events` | Python | table, `delete+insert` on `receivedate` | 15,633 |
| `openfda.stg_reports` | DuckDB SQL | view | 15,633 |
| `openfda.stg_reactions` | DuckDB SQL | view | 45,821 |
| `openfda.stg_drugs` | DuckDB SQL | view | 61,369 |
| `openfda.mart_daily_report_volume` | DuckDB SQL | table, `create+replace` | 5 |
| `openfda.mart_drug_reaction_signals` | DuckDB SQL | table, `create+replace` | 3,483 |

Six assets, 56 quality checks, one command.

```
openfda_raw.drug_events
└── openfda.stg_reports
    ├── openfda.stg_reactions ──┐
    ├── openfda.stg_drugs ──────┤
    ├── openfda.mart_daily_report_volume
    └── openfda.mart_drug_reaction_signals ◄┘
```

## What moved where

| | dlt + dbt original | this |
|---|---|---|
| Ingestion | `pipeline_dlt_rest.py`, `limit: 5`, `single_page`, `write_disposition="replace"` | Python asset, paged, one day at a time, `delete+insert` on `receivedate` |
| Transformation | `openfda_dbt/` with its own `dbt_project.yml` and `~/.dbt/profiles.yml` | SQL assets in the same project, same config file |
| Nested arrays | dlt child tables joined on `_dlt_id` / `_dlt_parent_id` | JSON kept in the raw layer, unnested in SQL, no vendor columns in the models |
| Tests | 8 `not_null` / `unique` in `.yml` sidecars | 56 checks inline in the asset that owns them, including 8 custom SQL checks |
| Scheduling | listed as a future step | `schedule`, `start_date`, `catchup` and `interval_modifiers` in `pipeline.yml` |
| Commands to run it | `pip install`, edit `profiles.yml`, `python3 pipeline_dlt_rest.py`, `dbt build` | `bruin run .` |

The point is not that dbt cannot express these models. It can. The difference
is that ingestion, transformation, tests, types, lineage, scheduling and
backfill live in one DAG that one binary runs, instead of two tools joined by a
DuckDB file and a README instruction to run them in the right order.

## Three things the data turned out to need

None of this was visible from the five-row sample. All three came out of
actually loading the source, and each one is now enforced by a check.

### 1. `patientonsetage` is not a number of years

The field is expressed in whichever unit `patientonsetageunit` names: decades,
years, months, weeks, days or hours. In the loaded window a value of `9` means
nine years on one row and ninety on another, and a value of `29935` means
eighty-two years in days.

The original model passes the column straight through as `patient_onset_age`.
Average it and you get **255**. Normalised to years, the same average is
**56.5**. `openfda.stg_reports` does the conversion and carries a non-blocking
check on implausible results.

### 2. `receivedate` is the first receipt, not the latest

FDA keeps revising cases under the same `safetyreportid` and the same
`receivedate`, bumping `safetyreportversion` and pushing `receiptdate` forward.
In this window **26.3% of cases are already past version 1**, and the newest
information on one of them arrived **851 days** after first receipt.

So `receivedate` is a safe incremental key, which is why it is the one used,
but a pipeline that only ever loads yesterday will hold stale versions of a
quarter of its cases forever. The asset carries a three-day
`interval_modifiers` lookback for genuinely late first arrivals, and catching
revisions needs a periodic wide re-pull on top:

```bash
# monthly, to pick up revisions to cases first received last quarter
bruin run . --start-date 2024-01-01 --end-date 2024-04-01
```

`delete+insert` makes that safe to repeat: re-running a window replaces it
rather than appending to it. Re-running the same two days twice leaves 1,210
rows both times.

### 3. 0.35% of cases produce 56% of the drug/reaction pairs

A case lists every drug the patient took and every reaction observed, and a
signal mart crosses the two. Most cases name one suspect drug. Some name 88.

Of 15,566 cases with a suspect drug, **55 name more than 20, and those 55
account for 56% of all drug/reaction pairs**. Left in, they bury the mart:
eleven of them mention "Rheumatic fever", which handed four unrelated drugs a
PRR above 4,700 and the entire top of the ranking.

`mart_drug_reaction_signals` excludes cases above
`max_suspect_drugs_per_case` (default 20) and computes every figure over the
remaining population, so the ratio stays internally consistent. That drops
13,240 pairs to 3,483 and leaves this at the top:

| Drug | Reaction | Cases | PRR |
|---|---|---|---|
| CAPECITABINE | Gastrointestinal injury | 12 | 2573 |
| ESCITALOPRAM | Sleep terror | 15 | 1450 |
| ELIGARD | Syringe issue | 26 | 886 |
| RIVASTIGMINE | Product adhesion issue | 14 | 857 |
| REMODULIN | Infusion site pain | 13 | 820 |
| YESCARTA | Immune effector cell-associated neurotoxicity syndrome | 10 | 815 |

These are labeled adverse effects, recovered from five days of raw reports.
Yescarta is a CAR-T therapy and ICANS is its hallmark toxicity. Remodulin is a
subcutaneous infusion and infusion site pain is its best known problem. Eligard
ships as a two-syringe mixing kit. Rivastigmine is a transdermal patch.
Finasteride, further down, pairs with decreased libido at PRR 37.

## Quality checks

Column checks are declared next to the column they describe, so the type, the
description and the constraint are one block. Custom checks are SQL:

```yaml
custom_checks:
  - name: no gaps in the loaded date range
    description: |
      Every day between the first and last loaded date should be present.
      A gap means a backfill interval failed and nobody noticed.
    query: |
      SELECT count(*) FROM ( ... ) WHERE expected_date NOT IN ( ... )
```

That one earned its place during development. A shell loop meant to backfill
two days silently did nothing, and the next run failed with
`custom check 'no gaps in the loaded date range' has returned 2 instead of the
expected 0` before anything downstream consumed the hole.

The checks that guard tolerances rather than absolutes state the tolerance
explicitly, because the honest bar is rarely zero:

- at least 95% of cases with a suspect drug survive the polypharmacy exclusion
- no more than 1% of cases lack a flagged suspect drug
- `reaction_index` is contiguous from 1, so the JSON unnest dropped nothing
- `pair_reports` never exceeds either of its margins

## Notes on the openFDA API

Things worth knowing, all found the hard way:

- Anonymous requests are capped below `limit=1000`. 500 works, 1000 returns
  `API_KEY_MISSING`. Set `OPENFDA_API_KEY` and raise the `page_size` variable.
- `skip` will not go past 25,000 for one query, which is why the asset walks a
  window one day at a time instead of asking for the whole range. A busy
  weekday is around 5,000 reports; a month is not fetchable in one query.
- An empty result set comes back as HTTP 404, not an empty array.
- The default `urllib` user agent gets a 403. `requests` is fine.
- Weekends are quiet: 4,565 reports on Thursday 29 February, 649 on Saturday
  2 March, 561 on Sunday 3 March.

## Caveats

FAERS is voluntary, unvalidated, duplicated and shaped by reporting campaigns.
`drug_name` is reporter free text, so one product appears under many
spellings, and no normalisation is attempted here. Report counts are not
incidence rates. PRR wants months of data before the ranking settles, and five
days is not that. A high PRR is a reason to look, not evidence of causation.
Do not use any of this for anything clinical.

## Credit

The pipeline, the source choice and the modelling approach are
[Peter Scheinsohn's](https://github.com/peterscheinsohn). This is his project
with the plumbing swapped out and his own next-steps list worked through.
