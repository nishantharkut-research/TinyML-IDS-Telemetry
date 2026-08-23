-- Exact hardware-execution counts and integer sums. Derive means downstream.
SELECT
  replay_run_id,
  lineage_id,
  board,
  student_model,
  'hardware_execution' AS statistical_unit,
  COUNT(*) AS hardware_execution_count,
  SUM(CASE WHEN status = 'OK' THEN 1 ELSE 0 END) AS successful_execution_count,
  SUM(CASE WHEN is_predicted_attack THEN 1 ELSE 0 END)
    AS predicted_attack_execution_count,
  SUM(CASE WHEN attempts > 1 THEN 1 ELSE 0 END) AS retry_event_count,
  SUM(GREATEST(attempts - 1, 0)) AS retry_count,
  SUM(response_timeout_count) AS response_timeout_count_sum,
  SUM(ignored_datagram_count) AS ignored_datagram_count_sum,
  SUM(preprocess_us) AS preprocess_us_sum,
  SUM(inference_us) AS inference_us_sum,
  SUM(total_us) AS total_us_sum,
  SUM(request_datagram_bytes_sent) AS request_datagram_bytes_sent_sum,
  SUM(response_bytes) AS response_bytes_sum,
  SUM(host_observed_datagram_rtt_us) AS host_observed_datagram_rtt_us_sum,
  SUM(transaction_elapsed_us) AS transaction_elapsed_us_sum
FROM finalized_gold
GROUP BY replay_run_id, lineage_id, board, student_model
