import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const pins = JSON.parse(
  readFileSync(new URL('../fixtures/ores-cli-peer-authority-pins.json', import.meta.url), 'utf8'),
);

const REQUIRED_COMPARE = ['typespec', 'schema', 'report', 'contract_ir', 'output_dir'];
const REQUIRED_VERIFY = [
  'typespec',
  'schema',
  'generated_schema',
  'contract_ir',
  'report',
  'expected_declarations',
  'verification',
];
const GENERATED_ROOTS = [
  '.typespec-json-schema-validator/',
  'evidence/',
  'generated/',
  'out/',
  'tmp/',
];

function normalized(path) {
  return String(path).replaceAll('\\', '/').replace(/^\.\//, '');
}

function requireBindings(binding, names) {
  const findings = [];
  for (const name of names) {
    if (typeof binding[name] !== 'string' || binding[name].trim() === '') {
      findings.push(`workflow-tjsv-required-input-missing:${name}`);
    }
  }
  return findings;
}

function isGenerated(path) {
  const value = normalized(path).toLowerCase();
  return value.includes('.typespec-json-schema-validator/')
    || value.includes('/generated/')
    || value.startsWith('generated/')
    || value.includes('typespec.generated.schema');
}

function within(path, directory) {
  const child = normalized(path);
  const parent = normalized(directory).replace(/\/$/, '');
  return parent !== '' && (child === parent || child.startsWith(`${parent}/`));
}

function auditComparison(binding) {
  const findings = requireBindings(binding, REQUIRED_COMPARE);
  if (binding.typespec === binding.schema) findings.push('workflow-tjsv-peer-input-alias');
  if (isGenerated(binding.schema)) findings.push('workflow-tjsv-authored-schema-generated');
  if (within(binding.schema, binding.output_dir)) {
    findings.push('workflow-tjsv-authored-schema-under-output');
  }
  return findings;
}

function auditVerification(binding) {
  const findings = requireBindings(binding, REQUIRED_VERIFY);
  if (binding.schema === binding.generated_schema) {
    findings.push('workflow-tjsv-verification-schema-alias');
  }
  if (binding.typespec === binding.schema) {
    findings.push('workflow-tjsv-verification-peer-alias');
  }
  if (isGenerated(binding.schema)) {
    findings.push('workflow-tjsv-verification-authored-generated');
  }
  if (!isGenerated(binding.generated_schema)) {
    findings.push('workflow-tjsv-generated-schema-unmarked');
  }
  return findings;
}

function auditPlacement(paths) {
  const findings = [];
  for (const raw of paths) {
    const path = normalized(raw);
    const lower = path.toLowerCase();
    const generatedRoot = GENERATED_ROOTS.some((root) => lower.startsWith(root));
    const authored = lower.endsWith('/main.tsp')
      || lower === 'main.tsp'
      || lower.endsWith('/authored.schema.json')
      || lower === 'authored.schema.json';
    const generated = lower.endsWith('/typespec.generated.schema.json')
      || lower.endsWith('/contract-ir.json')
      || lower.endsWith('/report.json')
      || lower.endsWith('/report.sarif');
    if (authored && generatedRoot) findings.push(`authored-authority-under-evidence:${path}`);
    if (generated && !generatedRoot) findings.push(`generated-evidence-in-authored-tree:${path}`);
  }
  return findings;
}

const goodCompare = {
  typespec: 'contracts/main.tsp',
  schema: 'contracts/authored.schema.json',
  report: '.typespec-json-schema-validator/report.json',
  contract_ir: '.typespec-json-schema-validator/contract-ir.json',
  output_dir: '.typespec-json-schema-validator/generated',
};
const goodVerify = {
  ...goodCompare,
  generated_schema: '.typespec-json-schema-validator/generated/typespec.generated.schema.json',
  expected_declarations: '["Example.Record"]',
  verification: '.typespec-json-schema-validator/verification.json',
};

test('source pins are immutable and zero-step product CI is not called certified', () => {
  assert.equal(pins.schemaVersion, 1);
  assert.equal(pins.sourceRepository, 'ORESoftware/ores-cli');
  for (const value of [
    pins.workflowBindingPr.head,
    pins.generatedEvidencePr.head,
    pins.contractTreePr.head,
    pins.validator,
  ]) assert.match(value, /^[0-9a-f]{40}$/);
  assert.equal(pins.claims.runtimeCertified, false);
  assert.equal(pins.claims.typespecFirstClass, true);
  assert.equal(pins.claims.jsonSchemaFirstClass, true);
  assert.equal(pins.claims.generatedSchemaAuthoritative, false);
});

test('complete comparison and verification bindings preserve both peer authorities', () => {
  assert.deepEqual(auditComparison(goodCompare), []);
  assert.deepEqual(auditVerification(goodVerify), []);
});

test('comparison requires an explicit TypeSpec authority', () => {
  const { typespec: _, ...missing } = goodCompare;
  assert.ok(auditComparison(missing).includes('workflow-tjsv-required-input-missing:typespec'));
});

test('comparison requires an explicit independently authored JSON Schema authority', () => {
  const { schema: _, ...missing } = goodCompare;
  assert.ok(auditComparison(missing).includes('workflow-tjsv-required-input-missing:schema'));
});

test('TypeSpec and authored JSON Schema paths cannot alias', () => {
  const findings = auditComparison({ ...goodCompare, schema: goodCompare.typespec });
  assert.ok(findings.includes('workflow-tjsv-peer-input-alias'));
});

test('generated Schema B cannot impersonate authored Schema A', () => {
  const findings = auditComparison({
    ...goodCompare,
    schema: '.typespec-json-schema-validator/generated/typespec.generated.schema.json',
  });
  assert.ok(findings.includes('workflow-tjsv-authored-schema-generated'));
  assert.ok(findings.includes('workflow-tjsv-authored-schema-under-output'));
});

test('verification requires distinct authored and generated JSON Schema paths', () => {
  const findings = auditVerification({
    ...goodVerify,
    schema: goodVerify.generated_schema,
  });
  assert.ok(findings.includes('workflow-tjsv-verification-schema-alias'));
  assert.ok(findings.includes('workflow-tjsv-verification-authored-generated'));
});

test('verification rejects an unmarked generated witness', () => {
  const findings = auditVerification({
    ...goodVerify,
    generated_schema: 'contracts/other.schema.json',
  });
  assert.ok(findings.includes('workflow-tjsv-generated-schema-unmarked'));
});

test('authored authorities and generated evidence occupy separate trees', () => {
  assert.deepEqual(auditPlacement([
    'contracts/main.tsp',
    'contracts/authored.schema.json',
    '.typespec-json-schema-validator/generated/typespec.generated.schema.json',
    '.typespec-json-schema-validator/contract-ir.json',
    'evidence/report.json',
    'evidence/report.sarif',
  ]), []);
  assert.deepEqual(auditPlacement([
    'generated/main.tsp',
    'evidence/authored.schema.json',
    'contracts/typespec.generated.schema.json',
    'contracts/contract-ir.json',
  ]), [
    'authored-authority-under-evidence:generated/main.tsp',
    'authored-authority-under-evidence:evidence/authored.schema.json',
    'generated-evidence-in-authored-tree:contracts/typespec.generated.schema.json',
    'generated-evidence-in-authored-tree:contracts/contract-ir.json',
  ]);
});
