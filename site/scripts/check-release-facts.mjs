import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';

export function validateManifest(facts) {
  if (facts.repository !== 'zpalmtree/seine' || !/^v\d+\.\d+\.\d+$/.test(facts.tag) ||
      !/^[a-f0-9]{40}$/.test(facts.commit) || !Number.isFinite(Date.parse(facts.publishedAt)) ||
      !Number.isFinite(Date.parse(facts.checkedAt))) throw new Error('Invalid release identity or verification date');
  if (facts.feePolicy !== undefined && !['flat', 'pool-specific'].includes(facts.feePolicy)) throw new Error('Invalid fee policy');
  if (facts.feePolicy === 'flat' && facts.bntpoolMinerFeePercent !== facts.otherPoolMinerFeePercent) throw new Error('Flat fee must match for every pool');
  for (const key of ['bntpoolMinerFeePercent', 'otherPoolMinerFeePercent']) {
    if (typeof facts[key] !== 'number' || !Number.isFinite(facts[key]) || facts[key] < 0 || facts[key] > 100) throw new Error(`Invalid ${key}`);
  }
  const platforms = ['seine-linux-aarch64.tar.gz', 'seine-linux-x86_64.tar.gz',
    'seine-macos-aarch64.tar.gz', 'seine-macos-x86_64.tar.gz', 'seine-windows-x86_64.zip'];
  const optional = [`seine-hiveos-${facts.tag.slice(1)}.tar.gz`, `seine-mmpos-${facts.tag.slice(1)}.tar.gz`];
  if (!Array.isArray(facts.assets) || new Set(facts.assets).size !== facts.assets.length ||
      !platforms.every(name => facts.assets.includes(name)) ||
      facts.assets.some(name => !platforms.includes(name) && !optional.includes(name))) {
    throw new Error('Expected reviewed platform archives and matching mining OS packages');
  }
  for (const path of ['src/dev_fee.rs', 'docs/guides/nvidia.md', '.github/workflows/release.yml', 'docs/guides/configuration.md']) {
    if (!/^[a-f0-9]{64}$/.test(facts.sourceHashes?.[path])) throw new Error(`Missing reviewed source hash: ${path}`);
  }
  if (!facts.reviewOnChange?.length) throw new Error('Missing review destinations');
}

export function compareRelease(facts, release, sources) {
  validateManifest(facts);
  if (release.tag_name !== facts.tag || release.published_at !== facts.publishedAt) throw new Error(`Published release changed: reviewed ${facts.tag}, latest ${release.tag_name}`);
  const archives = release.assets.map(x => x.name).filter(x => /^seine-.*\.(tar\.gz|zip)$/.test(x)).sort();
  if (JSON.stringify(archives) !== JSON.stringify([...facts.assets].sort())) throw new Error('Published platform archives changed');
  for (const [path, hash] of Object.entries(facts.sourceHashes)) {
    if (createHash('sha256').update(sources[path] ?? '').digest('hex') !== hash) throw new Error(`Reviewed release source changed: ${path}`);
  }
  const fees = sources['src/dev_fee.rs'];
  const standard = fees.match(/\bconst DEV_FEE_PERCENT:\s*f64\s*=\s*([\d.]+)/)?.[1];
  const pool = fees.match(/\bconst BNTPOOL_DEV_FEE_PERCENT:\s*f64\s*=\s*([\d.]+)/)?.[1];
  // A new fee policy needs explicit review rather than guessing a new default.
  if (standard === undefined || Number(standard) !== facts.otherPoolMinerFeePercent || (facts.feePolicy === 'flat' ? pool !== undefined : pool === undefined || Number(pool) !== facts.bntpoolMinerFeePercent)) throw new Error('Reviewed fee policy does not match released code');
}

async function main() {
  const args = process.argv.slice(2);
  const index = args.indexOf('--manifest');
  const file = index < 0 ? new URL('../editorial/release-facts.json', import.meta.url) : args[index + 1];
  const facts = JSON.parse(readFileSync(file, 'utf8'));
  try {
    validateManifest(facts);
    if (args.includes('--live')) {
      const get = async url => {
        const headers = { 'User-Agent': 'seine-content-facts' };
        if (url.startsWith('https://api.github.com/') && process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
        const response = await fetch(url, { headers, signal: AbortSignal.timeout(30000) });
        if (!response.ok) throw new Error(`${response.status} fetching ${url}`);
        return response;
      };
      const release = await (await get(`https://api.github.com/repos/${facts.repository}/releases/latest`)).json();
      const sources = Object.fromEntries(await Promise.all(Object.keys(facts.sourceHashes).map(async path => [path, await (await get(`https://raw.githubusercontent.com/${facts.repository}/${facts.tag}/${path}`)).text()])));
      compareRelease(facts, release, sources);
      const tag = await (await get(`https://api.github.com/repos/${facts.repository}/commits/${facts.tag}`)).json();
      if (tag.sha !== facts.commit) throw new Error('Reviewed release tag moved');
    }
    console.log(`Release facts valid: ${facts.tag}${args.includes('--live') ? ' (verified against GitHub)' : ' (offline manifest)'}`);
  } catch (error) {
    console.error(`${error.message}\nReview and update the affected content in all languages:\n${facts.reviewOnChange?.join('\n') ?? ''}`);
    process.exitCode = 1;
  }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await main();
