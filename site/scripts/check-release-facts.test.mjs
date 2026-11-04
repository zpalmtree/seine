import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { compareRelease, validateManifest } from './check-release-facts.mjs';
const manifest = JSON.parse(readFileSync(new URL('../editorial/release-facts.json', import.meta.url)));
function fixture() {
  const facts = structuredClone(manifest);
  const sources = Object.fromEntries(Object.keys(facts.sourceHashes).map(path => [path, path === 'src/dev_fee.rs' ? 'pub const DEV_FEE_PERCENT: f64 = 2.0;' : 'reviewed source']));
  facts.sourceHashes = Object.fromEntries(Object.entries(sources).map(([path, source]) => [path, createHash('sha256').update(source).digest('hex')]));
  return { facts, sources, release: { tag_name: facts.tag, published_at: facts.publishedAt, assets: facts.assets.map(name => ({name})) } };
}
test('accepts matching published facts', () => { const {facts,release,sources}=fixture(); compareRelease(facts,release,sources); });
test('rejects a newer release, missing archive, changed source and wrong fee', () => {
  for (const mutate of [x => x.release.tag_name='v99.0.0', x => x.release.assets.pop(), x => x.sources['docs/guides/nvidia.md']='changed', x => x.facts.bntpoolMinerFeePercent=3]) {
    const x=fixture();mutate(x);assert.throws(()=>compareRelease(x.facts,x.release,x.sources));
  }
});
test('rejects malformed facts instead of producing a calculator default', () => {
  for (const value of [null, '1', -1, 101, NaN, Infinity]) assert.throws(()=>validateManifest({...manifest,bntpoolMinerFeePercent:value}));
  assert.throws(()=>validateManifest({...manifest,sourceHashes:{}}));
});

test('flat policy refuses an unreviewed pool-specific override even with matching source hashes', () => {
  const x=fixture();
  x.sources['src/dev_fee.rs'] += '\npub const BNTPOOL_DEV_FEE_PERCENT: f64 = 1.0;';
  x.facts.sourceHashes['src/dev_fee.rs']=createHash('sha256').update(x.sources['src/dev_fee.rs']).digest('hex');
  assert.throws(()=>compareRelease(x.facts,x.release,x.sources), /fee policy/);
});
test('legacy pool-specific manifests remain checked against both fee constants', () => {
  const x=fixture(); delete x.facts.feePolicy;
  x.facts.bntpoolMinerFeePercent=1; x.facts.otherPoolMinerFeePercent=2.5;
  x.sources['src/dev_fee.rs']='pub const DEV_FEE_PERCENT: f64 = 2.5;\npub const BNTPOOL_DEV_FEE_PERCENT: f64 = 1.0;';
  x.facts.sourceHashes['src/dev_fee.rs']=createHash('sha256').update(x.sources['src/dev_fee.rs']).digest('hex');
  compareRelease(x.facts,x.release,x.sources);
});

test('reviews mining OS archives explicitly with matching release versions', () => {
  const x = fixture();
  const packages = ['hiveos', 'mmpos'].map(os => `seine-${os}-${x.facts.tag.slice(1)}.tar.gz`);
  // Start from the platform archives alone, whatever the manifest lists.
  x.facts.assets = x.facts.assets.filter(name => !packages.includes(name));
  x.release.assets = x.release.assets.filter(({ name }) => !packages.includes(name));
  x.release.assets.push(...packages.map(name => ({ name })));
  assert.throws(() => compareRelease(x.facts, x.release, x.sources), /archives changed/);
  x.facts.assets.push(...packages);
  compareRelease(x.facts, x.release, x.sources);
  for (const extra of [packages[0], 'seine-hiveos-99.0.0.tar.gz', 'seine-unknown.tar.gz']) {
    assert.throws(() => validateManifest({ ...x.facts, assets: [...x.facts.assets, extra] }));
  }
});
