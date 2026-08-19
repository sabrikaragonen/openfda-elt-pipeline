/* @bruin

type: duckdb.sql
description: |
  Suspect drug and reaction pairs with a Proportional Reporting Ratio, which
  is the first thing a pharmacovigilance team actually computes from FAERS.

      PRR = (a / (a + b)) / (c / (c + d))

  where a is cases naming this drug *and* this reaction, a+b is all cases
  naming the drug, c is cases naming the reaction without the drug, and c+d is
  all cases not naming the drug. PRR of 3 means the reaction is reported three
  times more often for this drug than for everything else in the dataset.

  Three decisions do most of the work:

  - Only `drug_role = 'suspect'` counts. Including concomitant drugs would
    attribute a reaction to every unrelated medication the patient was on.
  - Cases naming more than `max_suspect_drugs_per_case` suspect drugs are
    excluded. A case contributes every drug crossed with every reaction, so a
    submission listing 88 suspect drugs contributes thousands of pairs on its
    own. On the five days loaded for this repo, 55 of 15,566 cases (0.35%)
    name more than 20 suspect drugs and between them account for 56% of all
    drug/reaction pairs. Left in, they push meaningless pairs to the top of
    the PRR ranking: eleven such cases mentioning "Rheumatic fever" gave four
    unrelated drugs a PRR above 4,700.
  - Pairs below `min_pair_reports` are dropped, because PRR on two cases is
    noise. Raise the threshold as the loaded window grows.

  Every figure below is computed over the eligible population only, so the
  ratio stays internally consistent.

  Caveats worth stating out loud: FAERS is voluntary and unvalidated, report
  counts are not incidence rates, `drug_name` is reporter free text so the
  same product appears under several spellings, PRR needs months of data
  rather than days before the ranking settles, and a high PRR is a prompt to
  look rather than evidence of causation.

depends:
  - openfda.stg_reports
  - openfda.stg_reactions
  - openfda.stg_drugs

materialization:
  type: table
  strategy: create+replace

columns:
  - name: drug_name
    type: varchar
    description: Suspect drug as reported, not normalised.
    checks:
      - name: not_null
  - name: reaction_term
    type: varchar
    description: MedDRA preferred term.
    checks:
      - name: not_null
  - name: pair_reports
    type: bigint
    description: Cases naming both the drug and the reaction.
    checks:
      - name: not_null
      - name: positive
  - name: drug_reports
    type: bigint
    description: Cases naming the drug as suspect.
    checks:
      - name: positive
  - name: reaction_reports
    type: bigint
    description: Cases naming the reaction.
    checks:
      - name: positive
  - name: total_reports
    type: bigint
    description: Eligible cases in the loaded window, i.e. the PRR denominator population.
    checks:
      - name: positive
  - name: prr
    type: double
    description: Proportional Reporting Ratio. NULL when the reaction never occurs without the drug.
    checks:
      - name: non_negative

custom_checks:
  - name: pair count never exceeds its margins
    description: |
      a can never be larger than a+b or c+d. If it is, the distinct-per-case
      logic leaked duplicate rows into the join.
    query: |
      SELECT count(*) FROM openfda.mart_drug_reaction_signals
      WHERE pair_reports > drug_reports OR pair_reports > reaction_reports

  - name: drug totals never exceed the population
    query: |
      SELECT count(*) FROM openfda.mart_drug_reaction_signals
      WHERE drug_reports > total_reports OR reaction_reports > total_reports

  - name: the polypharmacy exclusion stays a trim, not a cull
    description: |
      The exclusion is meant to remove a fraction of a percent of cases. If
      max_suspect_drugs_per_case is set too low it would quietly discard most
      of the population and every ratio here would be computed on a rump.
      Fail if fewer than 95% of cases with a suspect drug survive it.
    query: |
      SELECT CASE WHEN (
        SELECT max(total_reports) FROM openfda.mart_drug_reaction_signals
      ) < 0.95 * (
        SELECT count(DISTINCT report_id) FROM openfda.stg_drugs
        WHERE drug_role = 'suspect'
      ) THEN 1 ELSE 0 END

@bruin */

WITH eligible_cases AS (
    -- A case qualifies if it names at least one suspect drug and few enough of
    -- them to be a focused report rather than a polypharmacy dump.
    SELECT report_id
    FROM openfda.stg_drugs
    WHERE drug_role = 'suspect'
    GROUP BY report_id
    HAVING count(DISTINCT drug_name)
        BETWEEN 1 AND {{ var.max_suspect_drugs_per_case }}
),

suspect_drugs AS (
    -- DISTINCT because a case can list the same drug more than once.
    SELECT DISTINCT d.report_id, d.drug_name
    FROM openfda.stg_drugs AS d
    JOIN eligible_cases USING (report_id)
    WHERE d.drug_role = 'suspect'
),

case_reactions AS (
    SELECT DISTINCT r.report_id, r.reaction_term
    FROM openfda.stg_reactions AS r
    JOIN eligible_cases USING (report_id)
),

population AS (
    SELECT count(*) AS total_reports FROM eligible_cases
),

drug_totals AS (
    SELECT drug_name, count(*) AS drug_reports
    FROM suspect_drugs
    GROUP BY drug_name
),

reaction_totals AS (
    SELECT reaction_term, count(*) AS reaction_reports
    FROM case_reactions
    GROUP BY reaction_term
),

pairs AS (
    SELECT d.drug_name, r.reaction_term, count(*) AS pair_reports
    FROM suspect_drugs AS d
    JOIN case_reactions AS r USING (report_id)
    GROUP BY d.drug_name, r.reaction_term
    HAVING count(*) >= {{ var.min_pair_reports }}
)

SELECT
    p.drug_name,
    p.reaction_term,
    p.pair_reports,
    dt.drug_reports,
    rt.reaction_reports,
    pop.total_reports,
    round(
        (p.pair_reports::DOUBLE / dt.drug_reports)
        / nullif(
            (rt.reaction_reports - p.pair_reports)::DOUBLE
            / nullif(pop.total_reports - dt.drug_reports, 0),
            0
        ),
        2
    ) AS prr
FROM pairs AS p
JOIN drug_totals AS dt USING (drug_name)
JOIN reaction_totals AS rt USING (reaction_term)
CROSS JOIN population AS pop
ORDER BY p.pair_reports DESC, prr DESC
