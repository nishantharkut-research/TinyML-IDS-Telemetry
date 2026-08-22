# Distributed Job Instructions

- Spark Bronze output preserves Kafka metadata and original payloads.
- Invalid payloads go to quarantine with deterministic validation reasons.
- Streaming deduplication is watermark-bounded. Gold finalization performs exact
  run-level deduplication.
- Use built-in Spark SQL expressions instead of Python UDFs where possible.
- Checkpoint paths are unique per query and never reused after incompatible
  query changes.
- Hadoop mapper and reducer use standard input/output and remain independently
  runnable without Spark imports.
- Exact integer counts and sums must reconcile between Spark and Hadoop.

