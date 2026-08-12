/* @bruin

name: openfda.stg_reports
type: duckdb.sql
description: |
  One row per adverse event case, decoded into readable values.

  Two things happen here that the raw layer deliberately leaves alone:

  1. **Age becomes comparable.** `patientonsetage` is expressed in whichever
     unit `patientonsetageunit` names, so the raw column mixes hours, days,
     months, decades and years in one number. In the loaded sample a value of
     `9` means nine years in one row and ninety in another, and `avg()` over
     the raw column returns 310. Here it is normalised to years, which brings
     the same average to 56.
  2. **Latest version wins.** The `QUALIFY` below keeps one row per case,
     ordered by version. This is defence in depth rather than a fix for a live
     duplicate: openFDA serves only the current version of each case, and the
     `delete+insert` strategy upstream replaces a whole `receivedate` on
     re-pull, so duplicates should not arise. Its value is that it turns the
     `unique` check below into an enforced invariant instead of an assumption
     about the source, and it keeps this model correct if the raw strategy is
     ever switched to `append` to retain revision history.

  Note the version cast in the raw asset matters here: `safetyreportversion`
  arrives as a string, and ordering it as text would rank version '2' above
  version '16'.

  `reactions` and `drugs` are carried through as JSON so the child assets
  inherit the latest-version filter instead of re-joining to raw.

depends:
  - openfda_raw.drug_events

materialization:
  type: view

columns:
  - name: report_id
    type: varchar
    description: openFDA safetyreportid, unique after the latest-version filter.
    checks:
      - name: not_null
      - name: unique
  - name: report_version
    type: integer
    description: Highest version of this case received so far.
    checks:
      - name: not_null
      - name: positive
  - name: received_date
    type: date
    description: Date FDA received the winning version of this case.
    checks:
      - name: not_null
  - name: receipt_date
    type: date
    description: Date of the most recent information for the case.
  - name: is_serious
    type: boolean
    description: NULL when openFDA did not classify the report.
  - name: is_death
    type: boolean
    description: True when the reported outcome was death.
    checks:
      - name: not_null
  - name: is_hospitalization
    type: boolean
    description: True when the event led to hospitalisation.
    checks:
      - name: not_null
  - name: patient_sex
    type: varchar
    description: Decoded from the openFDA numeric code.
    checks:
      - name: not_null
      - name: accepted_values
        value: ["male", "female", "unknown"]
  - name: patient_age_years
    type: double
    description: Age at onset in years, normalised from the source unit. NULL when age or unit is missing.
  - name: occur_country
    type: varchar
    description: Country where the event occurred.
  - name: reporter_country
    type: varchar
    description: Country of the reporter.
  - name: reactions
    type: varchar
    description: JSON array of reactions, unnested by openfda.stg_reactions.
  - name: drugs
    type: varchar
    description: JSON array of drugs, unnested by openfda.stg_drugs.

custom_checks:
  - name: no superseded versions survive
    description: |
      The dedup above must leave exactly one row per case. A duplicate here
      means the version cast failed or the window function changed shape.
    query: |
      SELECT count(*) FROM (
        SELECT report_id FROM openfda.stg_reports GROUP BY 1 HAVING count(*) > 1
      )

  - name: normalised ages are plausible
    description: |
      Non-blocking on purpose. FAERS is voluntarily reported and does contain
      impossible ages. This measures how many rather than failing the run, so
      the number is visible in run history instead of hidden.
    query: |
      SELECT count(*) FROM openfda.stg_reports
      WHERE patient_age_years > 120
    blocking: false

@bruin */

SELECT
    safetyreportid AS report_id,
    safetyreportversion AS report_version,
    receivedate AS received_date,
    receiptdate AS receipt_date,

    CASE serious WHEN '1' THEN TRUE WHEN '2' THEN FALSE END AS is_serious,
    coalesce(seriousnessdeath = '1', FALSE) AS is_death,
    coalesce(seriousnesshospitalization = '1', FALSE) AS is_hospitalization,

    CASE patientsex
        WHEN '1' THEN 'male'
        WHEN '2' THEN 'female'
        ELSE 'unknown'
    END AS patient_sex,

    -- 800 decade, 801 year, 802 month, 803 week, 804 day, 805 hour
    CASE patientonsetageunit
        WHEN '800' THEN patientonsetage * 10
        WHEN '801' THEN patientonsetage
        WHEN '802' THEN patientonsetage / 12
        WHEN '803' THEN patientonsetage / 52.1775
        WHEN '804' THEN patientonsetage / 365.25
        WHEN '805' THEN patientonsetage / 8766
    END AS patient_age_years,

    occurcountry AS occur_country,
    primarysourcecountry AS reporter_country,

    reactions,
    drugs

FROM openfda_raw.drug_events
QUALIFY row_number() OVER (
    PARTITION BY safetyreportid
    ORDER BY safetyreportversion DESC, receivedate DESC
) = 1
