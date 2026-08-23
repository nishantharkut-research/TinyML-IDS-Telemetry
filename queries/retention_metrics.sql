-- Offline policy evaluation. True labels are read here, never by the online policy.
SELECT
  decisions.replay_run_id,
  decisions.policy_id,
  'hardware_execution' AS statistical_unit,
  COUNT(*) AS hardware_execution_count,
  SUM(CASE WHEN decisions.retained THEN 1 ELSE 0 END)
    AS retained_hardware_execution_count,
  SUM(CASE WHEN gold.is_predicted_attack THEN 1 ELSE 0 END)
    AS predicted_attack_execution_count,
  SUM(CASE WHEN decisions.retained AND gold.is_predicted_attack THEN 1 ELSE 0 END)
    AS retained_predicted_attack_execution_count,
  SUM(CASE WHEN gold.true_class_id <> gold.normal_class_id THEN 1 ELSE 0 END)
    AS actual_attack_execution_count,
  SUM(
    CASE
      WHEN decisions.retained AND gold.true_class_id <> gold.normal_class_id THEN 1
      ELSE 0
    END
  ) AS retained_actual_attack_execution_count,
  SUM(decisions.inclusion_probability_numerator)
    AS inclusion_probability_numerator_sum,
  SUM(decisions.inclusion_probability_denominator)
    AS inclusion_probability_denominator_sum
FROM policy_decisions AS decisions
INNER JOIN finalized_gold AS gold
  ON decisions.replay_run_id = gold.replay_run_id
  AND decisions.source_event_id = gold.source_event_id
GROUP BY decisions.replay_run_id, decisions.policy_id
