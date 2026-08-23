# Distributed Job Instructions

- Spark Bronze output preserves Kafka metadata and original payloads.
- Invalid payloads go to quarantine with deterministic validation reasons.
- Streaming deduplication is watermark-bounded. Gold finalization performs exact
  run-level deduplication.
- Streaming correctness experiments use one Kafka partition. The watermark delay
  must strictly exceed the maximum timestamp skew derived from the replay interval
  and out-of-order window. Silver and streaming policy rows remain provisional.
- Gold is rebuilt from Bronze with the same strict validator. Finalization requires
  the immutable completion receipt and canonical snapshot, proves schedule and
  event equality in both directions, then publishes canonical snapshot rows.
- Use built-in Spark SQL expressions instead of Python UDFs where possible.
- Checkpoint paths are unique per query and never reused after incompatible
  query changes.
- Hadoop mapper and reducer use standard input/output and remain independently
  runnable without Spark imports.
- Exact integer counts and sums must reconcile between Spark and Hadoop.
