import { readFileSync, readdirSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';

const normalize = source => source.replace(/,/g, '.').replace(/\u00a0/g, ' ');
export function assertTableFact(source, chip, hps, threads) {
  const compact = value => value.replace(/[\s-]/g, '');
  const rows = normalize(source).split('\n').filter(line => line.startsWith('|') && compact(line).includes(compact(chip)));
  if (!rows.length || rows.some(row => {
    const cells = row.split('|').map(cell => cell.trim());
    return !cells.some(cell => Number(cell.replace(/\s*H\/s$/, '')) === hps)
      || (threads != null && !cells.includes(String(threads)));
  })) throw new Error(`Benchmark row drift: ${chip}, expected ${hps} H/s${threads == null ? '' : ` at ${threads} threads`}`);
}
export function assertPublicLinks(source) {
  if (/https:\/\/github\.com\/zpalmtree\/seine\/blob\/master\/(experiments|perf-results|site\/editorial\/batches)\//.test(source)) {
    throw new Error('Public article links to a private measurement record');
  }
}

export function checkBenchmarkFacts() {
  const root = new URL('../', import.meta.url);
  const site = JSON.parse(readFileSync(new URL('src/data/benchmarks.json', root)));
  const chart = JSON.parse(readFileSync(new URL('../docs/assets/charts/benchmarks.json', root)));
  for (const [i, row] of site.best.entries()) {
    if (Number(chart.best[i]?.hps.toFixed(1)) !== row.hps) throw new Error(`Site/chart drift: ${row.chip}`);
  }
  const best = site.best[0];
  for (const slug of ['cpu-mining-2026', 'how-to-mine-blocknet', 'how-crypto-mining-works', 'blocknet-mining-hardware-hashrate']) {
    const folder = new URL(`src/content/blog/${slug}/`, root);
    for (const file of readdirSync(folder).filter(x => x.endsWith('.mdx'))) {
      const source = readFileSync(new URL(file, folder), 'utf8').replace(/,/g, '.').replace(/\u00a0/g, ' ');
      if (!source.includes(best.chip) || !source.includes(`${best.hps} H/s`) || !source.includes(String(best.threads))) throw new Error(`Best measured result drift: ${slug}/${file}; review against benchmarks.json`);
      if (!source.split('\n\n').some(paragraph => paragraph.includes(best.chip) && paragraph.includes(String(best.memoryGiB)))) throw new Error(`Benchmark conditions missing: ${slug}/${file}`);
      if (slug === 'how-to-mine-blocknet' && !source.split('\n').some(line => line.includes('seine-linux-x86_64.tar.gz') && line.includes('NVRTC') && line.includes('2.39'))) throw new Error(`Linux runtime requirements missing: ${file}`);
    }
  }
  for (const slug of ['cpu-mining-2026', 'how-to-mine-blocknet', 'blocknet-mining-hardware-hashrate', 'mine-blocknet-nvidia-gpu']) {
    const folder = new URL(`src/content/blog/${slug}/`, root);
    for (const file of readdirSync(folder).filter(x => x.endsWith('.mdx'))) {
      const source = readFileSync(new URL(file, folder), 'utf8');
      try {
        if (slug !== 'mine-blocknet-nvidia-gpu') {
          const chips = slug === 'blocknet-mining-hardware-hashrate' ? ['EPYC 9R14', 'EPYC 9R45'] : ['EPYC 9R14'];
          for (const chip of chips) {
            const row = chart.servers.find(r => r.name === `AMD ${chip}`);
            assertTableFact(source, chip, Number(row.hps.toFixed(1)), 32);
          }
        }
        if (['blocknet-mining-hardware-hashrate', 'mine-blocknet-nvidia-gpu'].includes(slug)) {
          assertTableFact(source, site.gpu[0].chip, site.gpu[0].hps);
          if (source.split('\n').some(line => line.startsWith('|') && line.includes('RTX 5090') && line.includes('+'))) throw new Error('Combined GPU rate needs its own current measurement');
        }
      } catch (error) { throw new Error(`${slug}/${file}: ${error.message}`); }
    }
  }
  for (const slug of readdirSync(new URL('src/content/blog/', root))) {
    const folder = new URL(`src/content/blog/${slug}/`, root);
    for (const file of readdirSync(folder).filter(x => x.endsWith('.mdx'))) assertPublicLinks(readFileSync(new URL(file, folder), 'utf8'));
  }
  const zen4 = site.best.find(row => row.chip === 'EPYC 9554P');
  for (const file of ['../README.md', '../docs/PERFORMANCE.md']) {
    const source = readFileSync(new URL(file, root), 'utf8');
    if (!source.includes(`${zen4.hps} H/s`) || !source.includes(`${zen4.threads} mining threads`)) throw new Error(`EPYC 9554P figure/settings drift: ${file}`);
  }
  console.log('CPU/GPU benchmarks, public links and Linux requirements agree across all languages');
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) checkBenchmarkFacts();
