# Test Instructions

- Use synthetic fixtures or the small checked-in fixture. Do not copy the full
  CuKD-XAI evidence into this repository.
- Tests must prove behavior, not implementation details.
- Contract tests cover schema versions, field types, lineage, and serialization.
- Property tests cover identity determinism and retention invariants.
- Integration tests that need Docker must be marked `integration` and skip with
  a clear reason when Docker is unavailable.
- A test must explicitly prove that changing `true_label` cannot change an
  online retention decision.
- Full-evidence tests are opt-in and require `CUKD_XAI_ROOT`.

