# ORES CLI peer-authority lint contract

This is an independent `fiducia-cloud-test` consumer contract for the active
`ORESoftware/ores-cli` workflow-lint changes.

`main.tsp` and `authored.schema.json` are independently maintained authorities.
Neither file is generated from, subordinate to, or a fallback for the other.
The workflow emits JSON Schema B into a separate evidence tree, compares that
witness with authored JSON Schema A, verifies the complete current Contract IR,
and preserves the authored inputs unchanged.

The Node suite independently models the workflow-binding and generated-evidence
boundary rules. It does not replace Rust compilation, and it deliberately keeps
`runtimeCertified` false while the private ores-cli jobs execute zero steps.
