import test from 'node:test';
import assert from 'node:assert/strict';
import { assertTableFact, assertPublicLinks } from '../scripts/check-benchmark-facts.mjs';

test('accepts locale decimal separators and nonbreaking spaces', () => {
  assertTableFact('| EPYC 9R14 | 32 | 45,3\u00a0H/s |', 'EPYC 9R14', 45.3, 32);
  assertTableFact('| EPYC-9R14-Server | 32 | 45,3 H/s |', 'EPYC 9R14', 45.3, 32);
  assertTableFact('| RTX 5090 only | 13.8 |', 'RTX 5090', 13.8);
});
test('rejects stale, missing and duplicate contradictory measurements', () => {
  for (const source of ['| EPYC 9R14 | 32 | 38.2 H/s |', '| EPYC 9R14 | 16 | 45.3 H/s |', '',
    '| EPYC 9R14 | 32 | 45.3 H/s |\n| EPYC 9R14 | 32 | 38.2 H/s |']) {
    assert.throws(() => assertTableFact(source, 'EPYC 9R14', 45.3, 32), /row drift/);
  }
});
test('public references must not point at migrated measurement records', () => {
  assertPublicLinks('[Performance](https://github.com/zpalmtree/seine/blob/master/docs/PERFORMANCE.md)');
  assert.throws(() => assertPublicLinks('[Old evidence](https://github.com/zpalmtree/seine/blob/master/experiments/run/summary.json)'), /private measurement/);
});
