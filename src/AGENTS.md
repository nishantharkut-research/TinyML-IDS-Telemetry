# Source Code Instructions

- `contracts` owns stable types, validation, serialization, and identifiers.
- `evidence` may read CuKD-XAI artifacts but must never write below the evidence
  root.
- `replay` owns synthetic time and delivery. It must not change source identity.
- `policies` must be deterministic, versioned, and independent of true labels.
- `quality` computes explicit completeness, uniqueness, validity, and timeliness
  measures.
- Import direction is contracts -> evidence/policies/quality -> replay/CLI.
  Core modules must not import Spark, Streamlit, or Hadoop code.
- Every public behavior requires a failing test first.

