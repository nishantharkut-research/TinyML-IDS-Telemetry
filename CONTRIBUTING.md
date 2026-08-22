# Contributing

## Development Workflow

1. Work on a focused feature branch.
2. Keep the external CuKD-XAI evidence repository read-only.
3. Write a failing test before changing behavior.
4. Run focused tests, then the complete non-integration suite.
5. Run Ruff and mypy before opening a pull request.
6. Record distributed integration results separately from unit-test results.

## Pull Request Requirements

- State the behavior and evidence boundary changed.
- List exact verification commands and outcomes.
- Identify any new output, checkpoint, or schema version.
- Do not include local paths, credentials, source datasets, or generated data.
- Do not weaken the identity, provenance, retention, or claim constraints in
  `AGENTS.md`.

## Commit Style

Use concise Conventional Commit messages such as:

```text
feat(evidence): add immutable lineage adapter
test(policy): prove true-label independence
docs(architecture): define Bronze to Gold contracts
```

