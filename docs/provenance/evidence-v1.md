# Evidence Bundle v1

## Scope

The course system consumes four completed Wi-Fi hardware lineages from the
companion CuKD-XAI research. Each lineage contains 56,301 executions. The total
of 225,204 is an execution count over one shared test set, not a count of
independent predictive samples.

The source paper is not yet published. This repository therefore records the
evidence lineage without inventing a journal citation. A formal article citation
will be added after a stable bibliographic record exists.

## Frozen Identities

| Item | Value |
| --- | --- |
| Sealed report SHA-256 | `898ba47cb72528bd8e7cad56b4a7a26b2a6f87872a614a9fbc31f98f20810162` |
| Evidence bundle ID | `fb7b0cbc74d098f2c908b90151baff6a7a3f0a1c67c17159ce8ff599fe6e84b5` |
| Bundle artifact count | 31 |
| Bundle payload bytes | 37,241,392 |
| Canonical snapshot ID | `d4cfb79165bcc88e82f3aae0721d86b9cd5fd008f5567d11d5e6bc98e6dfb4ff` |
| Canonical event count | 225,204 |
| Canonical event SHA-256 | `dd790ff824e2c53e9661da9e03a6f0d05eb78fc61b36e10c57ac4b6d83314f8f` |
| Lineage count | 4 |
| Lineage SHA-256 | `92fe3ae37fadf616e7e9d7e3e043feec792a1d355cf044c87aac03245136daab` |

## Verification

The verified build performs these checks before publishing a snapshot:

1. validate the sealed report, completion manifests, compile records, export
   reports, export manifests, preprocessing metadata, and firmware hashes;
2. validate board, model, protocol, session, stage, dataset, split, scaler,
   class mapping, seed, and fixed-point output-scale lineage;
3. join every MCU row to its fixed-point reference by contiguous `row_id`;
4. require exact prediction and all-five-logit equality for every execution;
5. rebuild from the imported bundle and require byte-identical event and lineage
   streams relative to a direct read-only source build.

The opt-in regression is `tests/full_evidence/test_cukd_xai.py`. Set
`TINYML_IDS_EVIDENCE_ROOT` to the imported bundle before running it.
