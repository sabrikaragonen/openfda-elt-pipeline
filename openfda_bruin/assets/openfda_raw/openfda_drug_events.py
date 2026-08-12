"""@bruin
name: openfda_raw.drug_events
type: python
image: python:3.13
connection: duckdb-default
description: |
  Adverse drug event reports from the openFDA REST API, landed as-is.

  Field names match the openFDA response so this layer stays a faithful
  copy of the source. `patient.reaction[]` and `patient.drug[]` are kept as
  JSON text and unnested downstream by the staging assets.

  The asset pulls only the reports whose `receivedate` falls inside the run
  interval, so a daily schedule fetches one day and a backfill fetches the
  days it is asked for. The window is walked one day at a time because openFDA
  will not page past 25,000 results for a single query.

  `receivedate` is the date FDA *first* received the case and never changes,
  which is what makes it a safe incremental key. The catch is that a case
  keeps being revised long afterwards under the same `receivedate`: in the
  sample loaded for this repo, 26% of cases were already past version 1 and
  the newest information on one of them arrived 851 days after first receipt.
  The three-day lookback below only catches genuinely late first arrivals. See
  the README on why keeping FAERS current also needs a periodic wide re-pull.

interval_modifiers:
  start: -3d

materialization:
  type: table
  strategy: delete+insert
  incremental_key: receivedate

columns:
  - name: safetyreportid
    type: varchar
    description: openFDA case identifier. Not unique on its own, see safetyreportversion.
    checks:
      - name: not_null
  - name: safetyreportversion
    type: integer
    description: Version of this case. FDA re-sends corrected cases with a higher version.
    checks:
      - name: not_null
      - name: positive
  - name: receivedate
    type: date
    description: Date FDA received this version of the report. Incremental key.
    checks:
      - name: not_null
  - name: receiptdate
    type: date
    description: Date of the most recent information received for the case.
  - name: serious
    type: varchar
    description: "1 = serious, 2 = non-serious."
  - name: seriousnessdeath
    type: varchar
    description: "1 when the outcome was death, otherwise absent."
  - name: seriousnesshospitalization
    type: varchar
    description: "1 when the event led to hospitalisation, otherwise absent."
  - name: occurcountry
    type: varchar
    description: Country where the event occurred, two-letter code.
  - name: primarysourcecountry
    type: varchar
    description: Country of the reporter.
  - name: reporttype
    type: varchar
    description: "1 = spontaneous, 2 = report from study, 3 = other, 4 = not available."
  - name: patientsex
    type: varchar
    description: "0 = unknown, 1 = male, 2 = female."
  - name: patientonsetage
    type: double
    description: Age at onset, in the unit given by patientonsetageunit. Not comparable across rows.
  - name: patientonsetageunit
    type: varchar
    description: "800 = decade, 801 = year, 802 = month, 803 = week, 804 = day, 805 = hour."
  - name: patientagegroup
    type: varchar
    description: FDA-assigned age band, frequently absent.
  - name: reactions
    type: varchar
    description: JSON array of patient.reaction objects.
    checks:
      - name: not_null
  - name: drugs
    type: varchar
    description: JSON array of patient.drug objects.
    checks:
      - name: not_null
  - name: _ingested_at
    type: timestamp
    description: When this row was pulled from the API.

custom_checks:
  - name: report version is orderable as a number
    description: |
      Guards the downstream "latest version wins" logic. If versions ever
      arrive as non-numeric strings the cast lands NULL and the dedup in
      openfda.stg_reports silently picks an arbitrary row.
    query: SELECT count(*) FROM openfda_raw.drug_events WHERE safetyreportversion IS NULL

  - name: one row per report version per day
    description: A given case version should be received exactly once.
    query: |
      SELECT count(*) FROM (
        SELECT safetyreportid, safetyreportversion, receivedate
        FROM openfda_raw.drug_events
        GROUP BY 1, 2, 3
        HAVING count(*) > 1
      )
@bruin"""

import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import requests

BASE_URL = "https://api.fda.gov/drug/event.json"

# openFDA refuses to page past this offset for a single query, so a day whose
# total exceeds it cannot be fetched whole. We fail loudly rather than land a
# partial day and let the gap check downstream discover it later.
MAX_SKIP = 25000

# Anonymous callers get 240 requests/minute per IP. 0.3s between pages keeps
# us well under that even when several days are backfilled in one run.
SLEEP_BETWEEN_PAGES = 0.3

AGE_UNITS = {"800", "801", "802", "803", "804", "805"}


def _api_key() -> str | None:
    """Optional. Lifts the `limit` ceiling to 1000 and widens the daily quota."""
    key = os.environ.get("OPENFDA_API_KEY", "").strip()
    # An unset ${OPENFDA_API_KEY} in .bruin.yml arrives as the literal string.
    if not key or key.startswith("${"):
        return None
    return key


def _get(params: dict) -> dict | None:
    """One openFDA call. Returns None when the window holds no reports."""
    key = _api_key()
    if key:
        params = {**params, "api_key": key}

    for attempt in range(5):
        response = requests.get(BASE_URL, params=params, timeout=60)

        if response.status_code == 404:
            # openFDA answers an empty result set with 404 NOT_FOUND.
            return None

        if response.status_code == 429 or response.status_code >= 500:
            backoff = 2**attempt
            print(f"openFDA returned {response.status_code}, retrying in {backoff}s")
            time.sleep(backoff)
            continue

        response.raise_for_status()
        return response.json()

    raise RuntimeError(f"openFDA still failing after 5 attempts: {params}")


def _to_date(raw: str | None) -> date | None:
    if not raw:
        return None
    return datetime.strptime(raw, "%Y%m%d").date()


def _version(raw: str | None) -> int | None:
    """openFDA sends the version as a string, so '16' sorts below '2' unless cast."""
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _age(raw: str | None) -> float | None:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _flatten(report: dict, ingested_at: datetime) -> dict:
    patient = report.get("patient") or {}
    age_unit = patient.get("patientonsetageunit")

    return {
        "safetyreportid": report.get("safetyreportid"),
        "safetyreportversion": _version(report.get("safetyreportversion")),
        "receivedate": _to_date(report.get("receivedate")),
        "receiptdate": _to_date(report.get("receiptdate")),
        "serious": report.get("serious"),
        "seriousnessdeath": report.get("seriousnessdeath"),
        "seriousnesshospitalization": report.get("seriousnesshospitalization"),
        "occurcountry": report.get("occurcountry"),
        "primarysourcecountry": report.get("primarysourcecountry"),
        "reporttype": report.get("reporttype"),
        "patientsex": patient.get("patientsex"),
        "patientonsetage": _age(patient.get("patientonsetage")),
        "patientonsetageunit": age_unit if age_unit in AGE_UNITS else None,
        "patientagegroup": patient.get("patientagegroup"),
        "reactions": json.dumps(patient.get("reaction") or []),
        "drugs": json.dumps(patient.get("drug") or []),
        "_ingested_at": ingested_at,
    }


def _days() -> list[date]:
    """
    Bruin hands us a half-open interval [start, end); openFDA's
    `receivedate:[a TO b]` is inclusive at both ends. The last day we want is
    therefore the day containing the final instant *before* `end`, which keeps
    a daily run at one day whether Bruin ends the interval at 00:00:00 the
    next day or at 23:59:59 the same day.

    The window is then walked one day at a time rather than queried as a
    single range, because openFDA refuses to page past MAX_SKIP results for
    any one query. A single day comfortably fits under that; a month does not.
    Chunking per day is what lets a wide backfill or a lookback window work at
    all.
    """
    fmt = "%Y-%m-%dT%H:%M:%S"
    first = datetime.strptime(os.environ["BRUIN_START_DATETIME"], fmt).date()
    last = (
        datetime.strptime(os.environ["BRUIN_END_DATETIME"], fmt)
        - timedelta(microseconds=1)
    ).date()
    last = max(last, first)

    return [first + timedelta(days=n) for n in range((last - first).days + 1)]


def _fetch_day(day: date, page_size: int, ingested_at: datetime):
    """Yields one batch of flattened reports per API page for a single day."""
    stamp = day.strftime("%Y%m%d")
    search = f"receivedate:[{stamp} TO {stamp}]"

    first = _get({"search": search, "limit": page_size, "skip": 0})
    if first is None:
        print(f"{stamp}: no reports")
        return

    total = first["meta"]["results"]["total"]
    if total > MAX_SKIP:
        raise ValueError(
            f"{stamp} has {total} reports but openFDA will not page past "
            f"{MAX_SKIP} for one query, so this day cannot be loaded whole."
        )

    yield [_flatten(r, ingested_at) for r in first["results"]]
    fetched = len(first["results"])

    while fetched < total:
        time.sleep(SLEEP_BETWEEN_PAGES)
        page = _get({"search": search, "limit": page_size, "skip": fetched})
        if page is None or not page["results"]:
            break
        yield [_flatten(r, ingested_at) for r in page["results"]]
        fetched += len(page["results"])

    if fetched != total:
        raise RuntimeError(
            f"openFDA reported {total} results for {stamp} but returned "
            f"{fetched}. Refusing to land a partial day."
        )

    print(f"{stamp}: loaded {fetched} reports")


def materialize():
    page_size = json.loads(os.environ.get("BRUIN_VARS") or "{}").get("page_size", 500)

    if not _api_key() and page_size >= 1000:
        raise ValueError(
            "openFDA rejects limit >= 1000 without an API key. "
            "Set OPENFDA_API_KEY or lower the page_size variable."
        )

    days = _days()
    print(f"Fetching {len(days)} day(s): {days[0]} to {days[-1]}")

    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    loaded = 0

    for index, day in enumerate(days):
        if index:
            time.sleep(SLEEP_BETWEEN_PAGES)
        for batch in _fetch_day(day, page_size, ingested_at):
            loaded += len(batch)
            yield batch

    print(f"Loaded {loaded} reports across {len(days)} day(s)")
