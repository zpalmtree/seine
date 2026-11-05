import { readFile, readdir, mkdir, writeFile, rm } from 'node:fs/promises';
import { parse } from 'yaml';
import { scene, renderCover, writeCover } from './editorial-cover.mjs';

// Each tuple describes this article's concept, not measured values or protocol wiring.
// A new article must deliberately choose a cover; there is no generic title-card fallback.
const scenes = {
  'antivirus-flags-crypto-miner': ['shield','download','check'],
  'argon2id-proof-of-work-explained': ['memory','chip','block'],
  'benchmark-seine-miner': ['chart','chip','clock'],
  'blocknet-atomic-swaps': ['lock','coin','coin','swap'],
  'blocknet-encrypted-memos': ['message','key','lock'],
  'blocknet-explorer-guide': ['search','block','document'],
  'blocknet-faraday-monetary-policy': ['clock','coin','update'],
  'blocknet-faraday-privacy': ['shield','ring','key','orbit'],
  'blocknet-faraday-upgrade': ['update','block','shield'],
  'blocknet-fungibility': ['balance','coin','coin','swap'],
  'blocknet-id-pay-by-name': ['wallet','message','check'],
  'blocknet-mining-hardware-hashrate': ['chart','chip','gpu'],
  'blocknet-payment-links': ['message','wallet','coin'],
  'blocknet-payment-proof': ['document','wallet','check'],
  'blocknet-payment-visibility': ['eye','wallet','lock','orbit'],
  'blocknet-privacy-features': ['lock','ring','shield','orbit'],
  'blocknet-sign-verify-message': ['message','key','check'],
  'blocknet-supply': ['coin','block','clock'],
  'blocknet-transaction-explained': ['wallet','coin','document'],
  'blocknet-transaction-fees': ['balance','document','coin'],
  'blocknet-view-only-wallet': ['eye','key','wallet'],
  'blocknet-wallet-backup-restore': ['key','wallet','document'],
  'blocknet-wallet-for-mining': ['wallet','miner','key'],
  'blocknet-wallet-node-miner': ['block','wallet','miner'],
  'blocknet-wallet-not-syncing': ['update','wallet','block'],
  'bulletproofs-explained': ['shield','coin','check'],
  'configure-seine-miner': ['sliders','document','miner'],
  'cpu-mining-2026': ['chip','coin','miner'],
  'dandelion-plus-plus-explained': ['network','leaf','lock','orbit'],
  'how-crypto-mining-works': ['block','chip','coin'],
  'how-much-ram-to-mine-blocknet': ['memory','chip','sliders'],
  'how-to-mine-blocknet': ['miner','download','block'],
  'huge-pages-blocknet-mining': ['memory','sliders','chart'],
  'mine-blocknet-amd-gpu': ['gpu','sliders','check'],
  'mine-blocknet-hiveos-mmpos': ['server','gpu','check'],
  'mine-blocknet-nvidia-gpu': ['gpu','download','block'],
  'mine-blocknet-on-linux': ['miner','document','check'],
  'mine-blocknet-on-mac': ['laptop','chip','block'],
  'mine-blocknet-on-servers': ['server','memory','miner'],
  'blocknet-mining-farm-proxy': ['network','server','check'],
  'mine-blocknet-on-windows': ['miner','download','check'],
  'mine-crypto-on-old-pc-or-laptop': ['laptop','memory','leaf'],
  'mine-while-using-pc': ['miner','sliders','pause'],
  'miner-cpu-usage': ['chip','sliders','chart'],
  'miner-keeps-stopping': ['power','miner','search'],
  'mining-electricity-use': ['plug','chip','clock'],
  'mining-hashrate-dropping': ['chart','miner','search'],
  'proof-of-work-vs-proof-of-stake': ['balance','chip','coin'],
  'ring-signatures-explained': ['ring','key','shield','orbit'],
  'run-blocknet-node': ['server','block','network'],
  'seed-phrase-explained': ['key','document','wallet'],
  'seine-automatic-cpu-tuning': ['sliders','chip','chart'],
  'send-receive-blocknet': ['coin','wallet','wallet','swap'],
  'start-miner-automatically-windows': ['power','clock','miner'],
  'stealth-addresses-explained': ['wallet','key','lock','orbit'],
  'update-seine-miner': ['update','download','miner'],
  'what-is-a-privacy-coin': ['coin','eye','lock','orbit'],
  'what-is-blocknet': ['block','coin','shield','orbit'],
  'who-controls-blocknet': ['network','document','check','orbit'],
};
const screenshots = {
  'blocknet-desktop-wallet': '../public/assets/blog/blocknet-desktop-wallet-dashboard.jpg',
  'blocknet-transfer-time': '../public/assets/blog/blocknet-transfer-time.jpg',
  'get-first-blocknet': '../public/assets/blog/get-first-blocknet.jpg',
  'update-blocknet-wallet': '../public/assets/blog/update-blocknet-wallet.jpg',
  'seine-dashboard-explained': '../src/assets/dashboard.webp',
};
export function coverSource(slug) {
  if(screenshots[slug]) return { screenshot: screenshots[slug] };
  const config=scenes[slug];
  if(!config) throw new Error(`Assign an article-specific cover for ${slug} in scripts/blog-covers.mjs`);
  const [main,left,right,link]=config;
  return { svg: scene({main,left,right,link}) };
}
const root=new URL('../',import.meta.url);
const output=new URL('public/assets/blog/covers/',root);
const sources=new URL('src/content/blog/',root);
const covers={};
// This directory is exclusively generated; remove stale content hashes before rebuilding.
await rm(output,{recursive:true,force:true});
for(const slug of (await readdir(sources)).sort()) {
  const raw=await readFile(new URL(`${slug}/index.mdx`,sources),'utf8');
  if(parse(raw.split('---')[1]).draft) continue;
  const spec=coverSource(slug);
  const source=spec.screenshot ? await readFile(new URL(spec.screenshot,import.meta.url)) : Buffer.from(spec.svg);
  covers[slug]=await writeCover(slug,await renderCover(source,{screenshot:Boolean(spec.screenshot)}),output,'/assets/blog/covers/');
}
await mkdir(new URL('src/generated/',root),{recursive:true});
await writeFile(new URL('src/generated/blog-covers.json',root),JSON.stringify(covers,null,2)+'\n');
console.log(`Generated ${Object.keys(covers).length} editorial covers (1200px + 440px), shared across all locales`);
