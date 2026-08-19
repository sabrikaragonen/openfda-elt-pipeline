/* @bruin

type: duckdb.sql
description: |
  Daily shape of the incoming report stream: how many cases arrived, how many
  were serious, and how old the patients were.

  Rebuilt in full on every run rather than loaded incrementally. That is a
  deliberate choice: a corrected case arrives on a later `receivedate` and
  supersedes an earlier version, which restates the day the original landed.
  An incremental mart keyed on `received_date` would keep the stale counts.
  Full rebuild is cheap here; on a bigger source the same reasoning points at
  a windowed recompute rather than a plain append.

depends:
  - openfda.stg_reports

materialization:
  type: table
  strategy: create+replace

columns:
  - name: received_date
    type: date
    description: Date FDA received the reports.
    checks:
      - name: not_null
      - name: unique
  - name: reports
    type: bigint
    description: Distinct cases received that day, latest version only.
    checks:
      - name: not_null
      - name: positive
  - name: serious_reports
    type: bigint
    description: Cases openFDA classified as serious.
    checks:
      - name: non_negative
  - name: death_reports
    type: bigint
    description: Cases where the reported outcome was death.
    checks:
      - name: non_negative
  - name: serious_rate
    type: double
    description: serious_reports / reports.
    checks:
      - name: non_negative
      - name: max
        value: 1.0
  - name: median_patient_age_years
    type: double
    description: Median normalised age, over the cases that reported one.
  - name: reports_with_age
    type: bigint
    description: How many cases reported a usable age, so the median can be judged.
    checks:
      - name: non_negative

custom_checks:
  - name: no gaps in the loaded date range
    description: |
      Every day between the first and last loaded date should be present.
      A gap means a backfill interval failed and nobody noticed.
    query: |
      SELECT count(*) FROM (
        SELECT unnest(generate_series(
          (SELECT min(received_date) FROM openfda.mart_daily_report_volume),
          (SELECT max(received_date) FROM openfda.mart_daily_report_volume),
          INTERVAL 1 DAY
        ))::DATE AS expected_date
      ) days
      WHERE expected_date NOT IN (
        SELECT received_date FROM openfda.mart_daily_report_volume
      )

@bruin */

SELECT
    received_date,
    count(*) AS reports,
    count(*) FILTER (WHERE is_serious) AS serious_reports,
    count(*) FILTER (WHERE is_death) AS death_reports,
    round(count(*) FILTER (WHERE is_serious) / count(*)::DOUBLE, 4) AS serious_rate,
    round(median(patient_age_years), 1) AS median_patient_age_years,
    count(patient_age_years) AS reports_with_age
FROM openfda.stg_reports
GROUP BY received_date
