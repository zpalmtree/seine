import { LOCALES, localizePath } from '../i18n/locales';
import { useTranslations } from '../i18n/ui';
import { getPosts } from '../lib/blog';
import { REPO, SITE_URL } from '../lib/site';

// A plain-text map of the site for AI answer engines (llmstxt.org).
export async function GET() {
    const t = useTranslations('en');
    const posts = await getPosts('en');
    const lines = [
        '# seine', '', `> ${t('homeDescription')}`, '',
        'seine mines Blocknet, a privacy cryptocurrency whose proof of work is memory-hard Argon2id using 2 GiB per hash. It runs on CPUs, NVIDIA GPUs and AMD Radeon GPUs (RX 6000 and newer), tunes itself while mining, and defaults to the bntpool.com pool.', '',
        '## Links', '',
        `- [Download](${SITE_URL}/#download): archives for Windows, macOS and Linux from the latest GitHub release`,
        `- [Source and documentation](${REPO})`,
        `- [Performance results](${REPO}/blob/master/docs/PERFORMANCE.md)`,
        '- [bntpool.com](https://bntpool.com): the default mining pool',
        '- [Blocknet](https://blocknetcrypto.com): the coin', '',
        '## Guides', '',
        ...posts.map((p) => `- [${p.title}](${p.url}): ${p.description}`), '',
        '## Other languages', '',
        ...LOCALES.filter((x) => x.code !== 'en').map((x) => `- [${x.endonym}](${SITE_URL}${localizePath('/', x.code)})`),
    ];
    return new Response(lines.join('\n') + '\n', { headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
}
