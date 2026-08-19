/* @bruin

type: duckdb.sql
description: |
  One row per reported reaction, unnested from the JSON array on
  openfda.stg_reports. Reads from the staging view rather than the raw table,
  so reactions belonging to superseded case versions are already gone.

depends:
  - openfda.stg_reports

materialization:
  type: view

columns:
  - name: report_id
    type: varchar
    description: Case this reaction belongs to.
    checks:
      - name: not_null
  - name: received_date
    type: date
    description: Received date of the parent case, carried down for partitioning.
    checks:
      - name: not_null
  - name: reaction_index
    type: integer
    description: 1-based position of the reaction inside the source array.
    checks:
      - name: not_null
      - name: positive
  - name: reaction_term
    type: varchar
    description: MedDRA preferred term for the reaction.
    checks:
      - name: not_null
  - name: reaction_outcome
    type: varchar
    description: Decoded outcome. NULL when openFDA did not report one.
    checks:
      - name: accepted_values
        value:
          - recovered
          - recovering
          - not recovered
          - recovered with sequelae
          - fatal
          - unknown

custom_checks:
  - name: every case has at least one reaction
    description: |
      A case with no reaction is a case with nothing to report, so a
      non-zero count here means the JSON unnest dropped rows.
    query: |
      SELECT count(*) FROM openfda.stg_reports r
      WHERE NOT EXISTS (
        SELECT 1 FROM openfda.stg_reactions x WHERE x.report_id = r.report_id
      )

  - name: reaction index is contiguous from 1
    description: The unnest must preserve array order without gaps.
    query: |
      SELECT count(*) FROM (
        SELECT report_id
        FROM openfda.stg_reactions
        GROUP BY 1
        HAVING max(reaction_index) <> count(*)
      )

@bruin */

WITH exploded AS (
    SELECT
        report_id,
        received_date,
        unnest(
            list_transform(
                json_extract(reactions, '$[*]'),
                lambda element, position: {'position': position, 'element': element}
            )
        ) AS reaction
    FROM openfda.stg_reports
)

SELECT
    report_id,
    received_date,
    reaction.position AS reaction_index,
    json_extract_string(reaction.element, '$.reactionmeddrapt') AS reaction_term,

    -- 1 recovered, 2 recovering, 3 not recovered, 4 recovered with sequelae,
    -- 5 fatal, 6 unknown
    CASE json_extract_string(reaction.element, '$.reactionoutcome')
        WHEN '1' THEN 'recovered'
        WHEN '2' THEN 'recovering'
        WHEN '3' THEN 'not recovered'
        WHEN '4' THEN 'recovered with sequelae'
        WHEN '5' THEN 'fatal'
        WHEN '6' THEN 'unknown'
    END AS reaction_outcome

FROM exploded
