import assert from 'node:assert/strict';
import test from 'node:test';

const ROOT = 'oresoftware/typespec-json-schema-validator';
const VERIFY = 'oresoftware/typespec-json-schema-validator/actions/verify-contract-ir';
const SHA = '93dd73cb246a09b0a1c62d7192cc62ca6ecbe405';

function reference(line) {
  const trimmed = line.trimStart();
  if (trimmed.startsWith('#')) return null;
  const withoutDash = trimmed.startsWith('-') ? trimmed.slice(1).trimStart() : trimmed;
  if (!withoutDash.startsWith('uses:')) return null;
  const raw = withoutDash.slice('uses:'.length).trim().split(' #', 1)[0].trim();
  if (raw.length < 2) return raw;
  const first = raw.at(0);
  const last = raw.at(-1);
  return (first === last && (first === '"' || first === "'")) ? raw.slice(1, -1) : raw;
}

function auditWorkflow(text) {
  let comparisonSteps = 0;
  let verificationSteps = 0;
  for (const line of text.split(/\r?\n/u)) {
    const parsed = reference(line);
    if (!parsed?.includes('@')) continue;
    const source = parsed.slice(0, parsed.lastIndexOf('@')).toLowerCase();
    if (source === ROOT) comparisonSteps += 1;
    if (source === VERIFY) verificationSteps += 1;
  }
  const findings = [];
  if (comparisonSteps > 0 && verificationSteps === 0) {
    findings.push('workflow-tjsv-verification-step-missing');
  }
  if (verificationSteps > 0 && comparisonSteps === 0) {
    findings.push('workflow-tjsv-comparison-step-missing');
  }
  return { comparisonSteps, verificationSteps, findings };
}

function pair() {
  return `jobs:\n  parity:\n    steps:\n      - uses: ORESoftware/typespec-json-schema-validator@${SHA}\n      - uses: ORESoftware/typespec-json-schema-validator/actions/verify-contract-ir@${SHA}\n`;
}

test('comparison and current-input verification form one exact-head workflow pair', () => {
  assert.deepEqual(auditWorkflow(pair()), {
    comparisonSteps: 1,
    verificationSteps: 1,
    findings: [],
  });
});

test('comparison-only workflow fails closed', () => {
  const result = auditWorkflow(
    `steps:\n  - uses: ORESoftware/typespec-json-schema-validator@${SHA}\n`,
  );
  assert.deepEqual(result.findings, ['workflow-tjsv-verification-step-missing']);
});

test('verification-only workflow fails closed', () => {
  const result = auditWorkflow(
    `steps:\n  - uses: ORESoftware/typespec-json-schema-validator/actions/verify-contract-ir@${SHA}\n`,
  );
  assert.deepEqual(result.findings, ['workflow-tjsv-comparison-step-missing']);
});

test('producer and verifier in different workflow files do not establish one admission boundary', () => {
  const producer = auditWorkflow(
    `steps:\n  - uses: ORESoftware/typespec-json-schema-validator@${SHA}\n`,
  );
  const verifier = auditWorkflow(
    `steps:\n  - uses: ORESoftware/typespec-json-schema-validator/actions/verify-contract-ir@${SHA}\n`,
  );
  assert.ok(producer.findings.includes('workflow-tjsv-verification-step-missing'));
  assert.ok(verifier.findings.includes('workflow-tjsv-comparison-step-missing'));
});

test('multiple comparisons are admitted when the same workflow verifies current evidence', () => {
  const result = auditWorkflow(
    `steps:\n  - uses: ORESoftware/typespec-json-schema-validator@${SHA}\n  - uses: ORESoftware/typespec-json-schema-validator@${SHA}\n  - uses: ORESoftware/typespec-json-schema-validator/actions/verify-contract-ir@${SHA}\n`,
  );
  assert.equal(result.comparisonSteps, 2);
  assert.equal(result.verificationSteps, 1);
  assert.deepEqual(result.findings, []);
});

test('comments and unrelated actions do not create semantic pairs', () => {
  const result = auditWorkflow(
    `steps:\n  # uses: ORESoftware/typespec-json-schema-validator@${SHA}\n  - uses: actions/checkout@${SHA}\n`,
  );
  assert.deepEqual(result, { comparisonSteps: 0, verificationSteps: 0, findings: [] });
});

test('balanced quotes and inline comments preserve action identity', () => {
  const result = auditWorkflow(
    `steps:\n  - uses: 'ORESoftware/typespec-json-schema-validator@${SHA}' # producer\n  - uses: "ORESoftware/typespec-json-schema-validator/actions/verify-contract-ir@${SHA}" # verifier\n`,
  );
  assert.deepEqual(result.findings, []);
  assert.equal(result.comparisonSteps, 1);
  assert.equal(result.verificationSteps, 1);
});
