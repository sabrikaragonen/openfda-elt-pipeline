# openFDA adverse drug events, in Bruin

A pipeline for adverse drug event reports from the openFDA REST API, built with
[Bruin](https://github.com/bruin-data/bruin) and DuckDB.

## Purpose

**This exists to show how to rewrite a dlt + dbt project in Bruin.**

It is a fork of
[peterscheinsohn/openfda-elt-pipeline](https://github.com/peterscheinsohn/openfda-elt-pipeline),
an educational dlt + dbt + DuckDB project. This repo replaces the plumbing and
keeps the source, the destination and the modelling intent, so the two repos
sit side by side as a before and after:

| | |
|---|---|
| **Before**, dlt + dbt | [peterscheinsohn/openfda-elt-pipeline](https://github.com/peterscheinsohn/openfda-elt-pipeline) |
| **After**, Bruin | this repo |

The original's README ends with a list of what it would do next:

> incremental loading; analytical dbt marts; dashboard; orchestration with
> Airflow; historical tracking of updated reports.

The port works through that list, so you get to see what each of those looks
like in Bruin, on the real source rather than on a five-row sample. All
comparison links below are pinned to upstream commit
[`f9e1b68`](https://github.com/peterscheinsohn/openfda-elt-pipeline/tree/f9e1b689f7e162a13ba9574abd48376deed6f719)
so the numbers stay checkable.

Every figure comes from [`openfda_bruin/run.log`](./openfda_bruin/run.log), one
clean run against the live API.

## Tech stack

- **Bruin**, one Go binary. It brings its own `uv` and fetches its own Python
  interpreter, so nothing else is installed.
- **DuckDB**, same destination as the original.
- **openFDA REST API**, same source as the original.

That is the entire list. **No dlt and no dbt**, and no Airflow either: the
scheduling the original lists as a future step is `schedule` / `start_date` /
`catchup` in [`pipeline.yml`](./openfda_bruin/pipeline.yml).

```
openFDA REST API
→ Bruin Python asset      openfda_raw.drug_events, paged and incremental
→ DuckDB
→ Bruin SQL assets        3 staging views, 2 marts
→ Bruin quality checks    56, declared on the assets they check
```

Python still appears, but as an implementation detail of one asset rather than a
prerequisite. The ingestion asset declares `image: python:3.13` and lists
`requests` in [`pyproject.toml`](./openfda_bruin/pyproject.toml); Bruin resolves
both.

## Run it

```bash
curl -LsSf https://getbruin.com/install.sh | sh   # if you do not have it
bruin run openfda_bruin --start-date 2024-02-28 --end-date 2024-03-04
```

That is the whole setup. **You do not need Python installed.** On the machine
that produced `run.log`, the Python on `PATH` was 3.14.6, while the ingestion
asset declares `image: python:3.13` and ran on a standalone CPython 3.13.5 that
Bruin downloaded into its own cache. The system Python was never touched, and
neither was any global site-packages. Assets in one pipeline can pin different
Python versions without colliding.

There is also no profiles file, no `pip install`, no virtualenv to activate and
no separate build step. The DuckDB connection is committed in
[`.bruin.yml`](./.bruin.yml) because a local file path is not a secret; real
projects keep that file out of git.

Then poke at it:

```bash
bruin query -c duckdb-default -q "SELECT * FROM openfda.mart_daily_report_volume ORDER BY 1"
bruin run openfda_bruin/assets/openfda/stg_reports.sql --downstream
bruin validate openfda_bruin
```

## Naming and lineage

No asset declares its own name. Bruin derives it from the file path, so
`assets/openfda/stg_reports.sql` is the asset `openfda.stg_reports` and lands in
schema `openfda`, table `stg_reports`. The layout *is* the naming:

```
openfda_bruin/
├── pipeline.yml
├── pyproject.toml
└── assets/
    ├── openfda_raw/
    │   └── drug_events.py                 →  openfda_raw.drug_events
    └── openfda/
        ├── stg_reports.sql                →  openfda.stg_reports
        ├── stg_reactions.sql              →  openfda.stg_reactions
        ├── stg_drugs.sql                  →  openfda.stg_drugs
        ├── mart_daily_report_volume.sql   →  openfda.mart_daily_report_volume
        └── mart_drug_reaction_signals.sql →  openfda.mart_drug_reaction_signals
```

One less thing to keep in sync, and you can read the warehouse layout off `ls`.

Bruin then builds the DAG from the assets themselves, so asking for one asset
pulls in everything that depends on it, in order:

```
$ bruin run openfda_bruin/assets/openfda_raw/drug_events.py --downstream

Running:  openfda_raw.drug_events
Running:  openfda.stg_reports
Running:  openfda.stg_reactions
Running:  openfda.mart_daily_report_volume
Running:  openfda.stg_drugs
Running:  openfda.mart_drug_reaction_signals

PASS openfda_raw.drug_events ........
PASS openfda.stg_reports ...........
PASS openfda.stg_reactions ........
PASS openfda.mart_daily_report_volume ..........
PASS openfda.stg_drugs ........
PASS openfda.mart_drug_reaction_signals ...........

 ✓ Assets executed      6 succeeded
 ✓ Quality checks       56 succeeded
```

The whole graph, ingestion included:

```
openfda_raw.drug_events                       Python, delete+insert on receivedate
│
└── openfda.stg_reports                       view, decoded and deduped
    │
    ├── openfda.stg_reactions                 view, JSON unnest
    ├── openfda.stg_drugs                     view, JSON unnest
    │
    ├── openfda.mart_daily_report_volume      table, create+replace
    │     └── from stg_reports
    │
    └── openfda.mart_drug_reaction_signals    table, create+replace
          └── from stg_reports + stg_reactions + stg_drugs
```

The thing to notice is that the ingestion step is *in* the graph. Upstream, dlt
and dbt are separate programs and the dependency between them is a DuckDB file
plus a README telling you which to run first. Here `openfda_raw.drug_events` is
a node like any other, so `bruin run` orders everything, and a failure upstream
marks the downstream assets `UPSTREAM FAILED` instead of quietly letting them
run on stale data.

| Asset | Type | Materialisation | Rows |
|---|---|---|---|
| [`openfda_raw.drug_events`](./openfda_bruin/assets/openfda_raw/drug_events.py) | Python | table, `delete+insert` on `receivedate` | 15,633 |
| [`openfda.stg_reports`](./openfda_bruin/assets/openfda/stg_reports.sql) | DuckDB SQL | view | 15,633 |
| [`openfda.stg_reactions`](./openfda_bruin/assets/openfda/stg_reactions.sql) | DuckDB SQL | view | 45,821 |
| [`openfda.stg_drugs`](./openfda_bruin/assets/openfda/stg_drugs.sql) | DuckDB SQL | view | 61,369 |
| [`openfda.mart_daily_report_volume`](./openfda_bruin/assets/openfda/mart_daily_report_volume.sql) | DuckDB SQL | table, `create+replace` | 5 |
| [`openfda.mart_drug_reaction_signals`](./openfda_bruin/assets/openfda/mart_drug_reaction_signals.sql) | DuckDB SQL | table, `create+replace` | 3,483 |

## Size and setup, honestly

The port is **nine times more code**. It is not a "look how much less you have
to write" story, and pretending otherwise would be silly:

| | [dlt + dbt original](https://github.com/peterscheinsohn/openfda-elt-pipeline/tree/f9e1b689f7e162a13ba9574abd48376deed6f719) | Bruin port |
|---|---|---|
| Source files | 8 | 9 |
| Total lines | 111 | 996 |
| Lines of SQL logic | 19 | 183 |
| Lines of config / scaffolding | 34 in repo, plus `~/.dbt/profiles.yml` outside it | 55, all in repo |
| Quality checks | 8, in 2 sidecar `.yml` files | 56, inline in the asset that owns them |
| Data rows it can hold | 5, replaced every run | any date range, 15,633 over the 5 days loaded |

Those extra lines are buying pagination, retries, rate limiting, code decoding,
age normalisation, two marts, a PRR calculation and 48 more tests. Of the 640
lines across the five SQL assets, only **183 are SQL**. The other 457 are the
`@bruin` header: descriptions, column types and checks, sitting in the same file
as the query they describe.

Where the port is genuinely smaller is in the setup:

| | dlt + dbt original | Bruin port |
|---|---|---|
| Installed on your machine | Python, then `pip install dlt duckdb dbt-duckdb requests` | one binary |
| Files to hand-edit before first run | `~/.dbt/profiles.yml`, with an absolute path | none |
| Commands to a full build | 4 (`pip install`, edit profiles, `python3 pipeline_dlt_rest.py`, `dbt build`) | 1 (`bruin run`) |
| Steps that must be run in the right order by hand | 2 | 0 |
| Config files living outside the repo | 1 | 0 |

None of this means dbt cannot express these models. It can, and the SQL would
look much the same. The difference is that ingestion, transformation, tests,
types, lineage, scheduling and backfill are one DAG that one binary runs.

## What moved where

| | dlt + dbt original | this |
|---|---|---|
| Ingestion | [`pipeline_dlt_rest.py`](https://github.com/peterscheinsohn/openfda-elt-pipeline/blob/f9e1b689f7e162a13ba9574abd48376deed6f719/pipeline_dlt_rest.py), `limit: 5`, `single_page`, `write_disposition="replace"` | Python asset, paged, one day at a time, `delete+insert` on `receivedate` |
| Transformation | [`openfda_dbt/`](https://github.com/peterscheinsohn/openfda-elt-pipeline/tree/f9e1b689f7e162a13ba9574abd48376deed6f719/openfda_dbt) with its own `dbt_project.yml` and `~/.dbt/profiles.yml` | SQL assets in the same project, same config file |
| Nested arrays | dlt child tables joined on `_dlt_id` / `_dlt_parent_id` | JSON kept in the raw layer, unnested in SQL, no vendor columns in the models |
| Tests | 8 `not_null` / `unique` in `.yml` sidecars | 56 checks inline, including 8 custom SQL checks |
| Scheduling | listed as a future step | `schedule`, `start_date`, `catchup` and `interval_modifiers` in `pipeline.yml` |
| Python environment | yours, whatever version it happens to be | declared per asset, fetched by Bruin |

## Three things the data turned out to need

None of this was visible from the five-row sample. All three came out of
actually loading the source, and each one is now enforced by a check.

### 1. `patientonsetage` is not a number of years

The field is expressed in whichever unit `patientonsetageunit` names: decades,
years, months, weeks, days or hours. In the loaded window a value of `9` means
nine years on one row and ninety on another, and `29935` means eighty-two years
in days.

The original model passes the column straight through as `patient_onset_age`.
Average it and you get **255**. Normalised to years, the same average is
**56.5**. [`openfda.stg_reports`](./openfda_bruin/assets/openfda/stg_reports.sql)
does the conversion and carries a check on implausible results.

### 2. `receivedate` is the first receipt, not the latest

FDA keeps revising cases under the same `safetyreportid` and the same
`receivedate`, bumping `safetyreportversion` and pushing `receiptdate` forward.
In this window **26.3% of cases are already past version 1**, and the newest
information on one of them arrived **851 days** after first receipt.

So `receivedate` is a safe incremental key, which is why it is the one used, but
a pipeline that only ever loads yesterday will hold stale versions of a quarter
of its cases forever. The asset carries a three-day `interval_modifiers`
lookback for genuinely late first arrivals, and catching revisions needs a
periodic wide re-pull on top:

```bash
# monthly, to pick up revisions to cases first received last quarter
bruin run openfda_bruin --start-date 2024-01-01 --end-date 2024-04-01
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

[`mart_drug_reaction_signals`](./openfda_bruin/assets/openfda/mart_drug_reaction_signals.sql)
excludes cases above `max_suspect_drugs_per_case` (default 20) and computes
every figure over the remaining population, so the ratio stays internally
consistent. That drops 13,240 pairs to 3,483 and leaves this at the top:

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

That one earned its place during development. A shell loop meant to backfill two
days silently did nothing, and the next run failed with
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
  window one day at a time instead of asking for the whole range. A busy weekday
  is around 5,000 reports; a month is not fetchable in one query.
- An empty result set comes back as HTTP 404, not an empty array.
- The default `urllib` user agent gets a 403. `requests` is fine.
- Weekends are quiet: 4,565 reports on Thursday 29 February, 649 on Saturday
  2 March, 561 on Sunday 3 March.

## Caveats

FAERS is voluntary, unvalidated, duplicated and shaped by reporting campaigns.
`drug_name` is reporter free text, so one product appears under many spellings,
and no normalisation is attempted here. Report counts are not incidence rates.
PRR wants months of data before the ranking settles, and five days is not that.
A high PRR is a reason to look, not evidence of causation. Do not use any of
this for anything clinical.

## Credit

The source choice, the modelling approach and the original pipeline are
[Peter Scheinsohn's](https://github.com/peterscheinsohn). This repo is his
project with the plumbing swapped out and his own next-steps list worked
through. His dlt and dbt code is not vendored here; it lives in
[the upstream repo](https://github.com/peterscheinsohn/openfda-elt-pipeline),
which is the thing to compare this against.
