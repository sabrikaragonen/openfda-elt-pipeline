/* @bruin

type: duckdb.sql
description: |
  One row per drug named on a case, unnested from the JSON array on
  openfda.stg_reports.

  `drug_role` matters for any downstream analysis: a case lists both the drug
  suspected of causing the reaction and every other drug the patient happened
  to be taking. Counting them together is the classic way to produce a
  nonsense safety signal, so the role is decoded here and filtered later.

depends:
  - openfda.stg_reports

materialization:
  type: view

columns:
  - name: report_id
    type: varchar
    description: Case this drug belongs to.
    checks:
      - name: not_null
  - name: received_date
    type: date
    description: Received date of the parent case.
    checks:
      - name: not_null
  - name: drug_index
    type: integer
    description: 1-based position of the drug inside the source array.
    checks:
      - name: not_null
      - name: positive
  - name: drug_name
    type: varchar
    description: Medicinal product name as reported, free text and not normalised by FDA.
    checks:
      - name: not_null
  - name: active_substance
    type: varchar
    description: Active substance name where reported.
  - name: is_serious
    type: boolean
    description: Seriousness of the parent case, carried down so metrics can filter locally.
  - name: patient_age_years
    type: double
    description: Normalised patient age of the parent case, carried down for the same reason.
  - name: drug_role
    type: varchar
    description: Whether the drug is suspected of causing the reaction.
    checks:
      - name: accepted_values
        value: ["suspect", "concomitant", "interacting"]
  - name: drug_indication
    type: varchar
    description: Condition the drug was taken for, where reported.

custom_checks:
  - name: every case has at least one drug
    description: A drug event report with no drug means the unnest dropped rows.
    query: |
      SELECT count(*) FROM openfda.stg_reports r
      WHERE NOT EXISTS (
        SELECT 1 FROM openfda.stg_drugs x WHERE x.report_id = r.report_id
      )

  - name: suspect drug is flagged on almost every case
    description: |
      Reporters are supposed to mark which drug is suspected, but a small
      fraction never do, and those cases drop out of the signal mart. So the
      bar is a tolerance rather than zero: fail if more than 1% of cases have
      no suspect drug, which is what a broken `drugcharacterization` decode or
      an upstream field rename would look like.
    query: |
      SELECT CASE WHEN (
        SELECT count(*) FROM openfda.stg_reports r
        WHERE NOT EXISTS (
          SELECT 1 FROM openfda.stg_drugs x
          WHERE x.report_id = r.report_id AND x.drug_role = 'suspect'
        )
      ) > 0.01 * (SELECT count(*) FROM openfda.stg_reports)
      THEN 1 ELSE 0 END

@bruin */

WITH exploded AS (
    SELECT
        report_id,
        received_date,
        is_serious,
        patient_age_years,
        unnest(
            list_transform(
                json_extract(drugs, '$[*]'),
                lambda element, position: {'position': position, 'element': element}
            )
        ) AS drug
    FROM openfda.stg_reports
)

SELECT
    report_id,
    received_date,
    is_serious,
    patient_age_years,
    drug.position AS drug_index,
    json_extract_string(drug.element, '$.medicinalproduct') AS drug_name,
    json_extract_string(drug.element, '$.activesubstance.activesubstancename') AS active_substance,

    -- 1 suspect, 2 concomitant, 3 interacting
    CASE json_extract_string(drug.element, '$.drugcharacterization')
        WHEN '1' THEN 'suspect'
        WHEN '2' THEN 'concomitant'
        WHEN '3' THEN 'interacting'
    END AS drug_role,

    json_extract_string(drug.element, '$.drugindication') AS drug_indication

FROM exploded
