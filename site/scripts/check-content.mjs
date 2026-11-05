#!/usr/bin/env node
// Editorial and translation checks for src/content/blog. Runs before every build.
//
//   npm run check                         every post; errors fail, warnings print
//   npm run check -- <slug> --strict      one post; warnings fail too (use before publishing)
//   npm run overlap -- <slug>             closest existing posts by shared wording
//   npm run check -- --hash <slug>        the sourceHash its translations must carry
//   npm run check -- <slug> --locale es   only the es translation (comma-separate several)
//
// Errors: forbidden claims, a missing keyword-map row, an H1 in the body, broken internal
// links, a missing or stale translation, or a translation whose structure
// (FAQ count, blocks, code, headings, external links) doesn't match its source.
// Warnings: the length and structure rules in editorial/README.md.
// The overlap report is advisory: it finds shared eight-word phrases, not shared intent.
import { createHash } from 'node:crypto';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { parse } from 'yaml';

const ROOT = new URL('../', import.meta.url);
const BLOG = new URL('src/content/blog/', ROOT);
// Every translated locale (src/i18n/locales.ts lists the same set).
const LOCALES = ['id', 'es', 'de', 'fr', 'pt-BR', 'ru', 'it', 'pl', 'tr', 'vi', 'ar', 'nl', 'uk', 'el', 'ja', 'ko', 'th', 'zh-Hans', 'zh-Hant']
    .map((code) => ({ code, path: code.toLowerCase() }));
const HOME_ANCHORS = ['download', 'start', 'performance', 'why'];
const BLOCKS = ['Answer', 'Note', 'Warning'];
const ctaCount = (b) => (stripCode(b).match(/<CTA\b/g) ?? []).length;

// Brand rules (marketing/brand/taglines.md, seine copy rules). [pattern, advice]
const FORBIDDEN = [
    // Translations: the same bans in every locale's usual wording.
    [/\buntraceable\b|不可追[踪蹤]|无法追[踪蹤]|無法追[踪蹤]|追跡不可能|추적\s*불가|irrastreab|irrastreáve|intraçable|unverfolgbar|nicht (nach)?verfolgbar|non tracciabil|niewykrywaln|izlenemez|không thể truy vết|untraceer|tidak (dapat|bisa) dilacak|неотслеживаем|невідстежуван|μη ανιχνεύσιμ|ไม่สามารถติดตาม|لا يمكن تتبع/i, "say 'private' (regulator and exchange risk)"],
    [/\banonym|\banónim|\banônim|\banonim|匿名|익명|nặc danh|ẩn danh|аноним|анонім|ανώνυμ|ไม่ระบุตัวตน|مجهول الهوية|إخفاء الهوية/i, "say 'private' (regulator and exchange risk)"],
    [/\bmost private\b|\bmore private than monero\b|最私密|比\s*Monero\s*更私密/i, 'false once Monero ships FCMP++'],
    [/\bguaranteed (profit|returns?|income)\b|\bpassive income\b|\brisk[- ]free\b|稳赚|穩賺|保证收益|保證收益|被动收入|被動收入|ingresos? pasivos?|renda passiva|passives? Einkommen|revenus? passifs?|reddito passivo|dochód pasywny|pasif gelir|thu nhập thụ động|passief inkomen|pendapatan pasif|пассивн\S* доход|пасивн\S* дох|παθητικό εισόδημα|不労所得|パッシブインカム|불로소득|รายได้(แบบ)?พาสซีฟ|دخل سلبي/i, 'no earnings promises'],
    [/blocknet\.org/i, 'a different project (the BLOCK token); never link it'],
];
const CAUTION = [
    [/\bworkers?\b/gi, "seine calls them threads (--threads); 'worker' is fine only for a pool worker name"],
    [/\bprofitab(le|ility)\b|盈利|获利|獲利/gi, 'profitability depends on price, difficulty and power cost; show the arithmetic, not a verdict'],
    [/\bASIC[- ](proof|resistant)\b/gi, "say 'designed to resist ASICs'"],
    [/\bfaster every release\b/gi, "don't promise future speedups"],
    // Copy stays favorable to Blocknet, bntpool.com and seine (editorial/writing-guide.md).
    [/\b(experimental|drawbacks?|downsides?|limitations?|unfortunately|not recommended|caveats?|risky|worse than|slower than|isn't worth|not worth)\b|缺点|缺點|局限|不推荐|不推薦|遗憾|遺憾|风险|風險|实验性|實驗性/gi, 'keep copy favorable: rewrite or drop unless the reader needs it to complete the task'],
];

const args = process.argv.slice(2);
const strict = args.includes('--strict');
const overlapMode = args.includes('--overlap');
const hashIdx = args.indexOf('--hash');
const wanted = args.filter((a, i) => !a.startsWith('--') && args[i - 1] !== '--locale' && (hashIdx < 0 || i !== hashIdx + 1));
const onlyLocales = args.flatMap((a, i) => (args[i - 1] === '--locale' ? a.split(',') : []));

function readPost(slug, file) {
    const url = new URL(`${slug}/${file}`, BLOG);
    if (!existsSync(url)) return null;
    const raw = readFileSync(url, 'utf8');
    const m = raw.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
    if (!m) return { slug, file, raw, error: 'missing front matter' };
    let data;
    try { data = parse(m[1]) ?? {}; } catch (e) { return { slug, file, raw, error: `front matter is not valid YAML: ${e.message}` }; }
    return { slug, file, raw, data, body: m[2] };
}

// The draft flag is left out so publishing a post doesn't make its translations stale.
const sourceHash = (raw) => createHash('sha256').update(raw.replace(/^draft:.*\n/m, '')).digest('hex').slice(0, 12);
const stripCode = (s) => s.replace(/```[\s\S]*?```/g, ' ');
const prose = (body) => stripCode(body).replace(/\]\([^)]*\)/g, ']').replace(/<[^>]+>/g, ' ');
const words = (s) => (s.match(/[A-Za-z0-9][\w'’.-]*/g) ?? []).length;
const blocks = (body, kind) => [...body.matchAll(new RegExp(`<${kind}>([\\s\\S]*?)</${kind}>`, 'g'))].map((m) => m[1]);
const links = (body) => [...stripCode(body).matchAll(/\]\(([^)\s]+)\)/g)].map((m) => m[1]);
const headings = (body) => [...stripCode(body).matchAll(/^(#{2,3}) (.+)$/gm)].map((m) => m[2]);

const slugs = readdirSync(BLOG, { withFileTypes: true }).filter((d) => d.isDirectory()).map((d) => d.name).sort();
const sources = slugs.map((s) => readPost(s, 'index.mdx')).filter(Boolean);
const published = new Set(sources.filter((p) => p.data && !p.data.draft).map((p) => p.slug));
const existing = new Set(sources.map((p) => p.slug));
// A published post may only link to published posts; a draft may also link to other drafts.
const linkable = (p) => (p.data?.draft ? existing : published);
const keywordMap = existsSync(new URL('editorial/keyword-map.md', ROOT)) ? readFileSync(new URL('editorial/keyword-map.md', ROOT), 'utf8') : '';

if (hashIdx >= 0) {
    const p = sources.find((s) => s.slug === args[hashIdx + 1]);
    if (!p) { console.error(`unknown post: ${args[hashIdx + 1]}`); process.exit(1); }
    console.log(sourceHash(p.raw));
    process.exit(0);
}

function checkLinks(p, prefix, errors) {
    for (const href of links(p.body)) {
        if (!href.startsWith('/')) continue;
        // Images and files under /assets/ just have to exist in public/.
        if (href.startsWith('/assets/')) {
            if (!existsSync(new URL(`public${href.split(/[?#]/)[0]}`, ROOT))) errors.push(`missing file ${href} (put it in site/public${href})`);
            continue;
        }
        if (prefix && !href.startsWith(`/${prefix}/`)) {
            errors.push(`internal link ${href} isn't localized; use /${prefix}${href}`);
            continue;
        }
        const path = prefix ? href.slice(prefix.length + 1) : href;
        const [base, frag] = path.split('#');
        if (base === '/' && frag && !HOME_ANCHORS.includes(frag)) errors.push(`link to missing home section #${frag}`);
        else if (base.startsWith('/blog/') && base !== '/blog/') {
            const target = base.slice(6).replace(/\/$/, '');
            if (!linkable(p).has(target)) errors.push(`link to unknown or draft post ${href}`);
            else if (!base.endsWith('/')) errors.push(`write ${href} with a trailing slash`);
        } else if (!['/', '/blog/'].includes(base)) errors.push(`link to unknown page ${href}`);
    }
}

const QUANTUM = /\bpost[- ]quantum\b|\bquantum[- ](resistant|proof|safe)\b|抗量子|后量子|後量子/i;
const FARADAY = /faraday|blocknetcrypto\.com/i;

function claims(text, errors, warnings) {
    for (const [re, advice] of FORBIDDEN) { const m = text.match(re); if (m) errors.push(`forbidden claim '${m[0]}': ${advice}`); }
    for (const [re, advice] of CAUTION) for (const m of text.matchAll(re)) warnings.push(`check wording '${m[0]}': ${advice}`);
    // Post-quantum is fine as Faraday's plan, attributed; flag sentences that don't say so.
    for (const sentence of text.split(/(?<=[.!?。！？])\s*|\n+/)) {
        const m = sentence.match(QUANTUM);
        if (m && !FARADAY.test(sentence)) warnings.push(`'${m[0]}' without Faraday/blocknetcrypto.com in the sentence: today's chain is classical crypto, so attribute it to the Faraday plan`);
    }
}

function checkSource(p) {
    const errors = [], warnings = [];
    const d = p.data;
    for (const k of ['title', 'description', 'date']) if (!d[k]) errors.push(`front matter needs '${k}'`);
    const faq = d.faq ?? [];
    claims([prose(p.body), d.title, d.description, ...faq.flatMap((f) => [f.q, f.a])].join(' '), errors, warnings);
    if (!d.draft && !keywordMap.includes(`/blog/${p.slug}/`)) errors.push(`no row for /blog/${p.slug}/ in editorial/keyword-map.md`);
    if (/^# /m.test(stripCode(p.body))) errors.push("the body has an H1 ('# '); the title is the only H1, start at '##'");
    if (ctaCount(p.body) !== 1) errors.push(`${ctaCount(p.body)} <CTA> blocks; place exactly one near the end (target="seine" or "pool", with title and text)`);
    for (const f of faq) if (/[`*[\]<>\n]/.test(f.a ?? '')) errors.push(`FAQ answer must be one line of plain text: ${f.q}`);
    for (const s of d.related ?? []) if (!linkable(p).has(s)) errors.push(`related post '${s}' doesn't exist or is a draft`);
    checkLinks(p, null, errors);

    const n = (d.description ?? '').length;
    if (n < 140 || n > 160) warnings.push(`description is ${n} characters; aim for 140-160`);
    if ((d.title ?? '').length > 60 && !d.seoTitle) warnings.push(`title is ${d.title.length} characters; add a seoTitle of 60 or fewer`);
    if (d.seoTitle && d.seoTitle.length > 60) warnings.push(`seoTitle is ${d.seoTitle.length} characters; keep it to 60`);
    const tags = d.tags ?? [];
    if (tags.length < 2 || tags.length > 6) warnings.push(`${tags.length} tags; use 2-6`);
    if (faq.length < 3 || faq.length > 5) warnings.push(`${faq.length} FAQ entries; use 3-5 real questions`);
    for (const f of faq) {
        if (!String(f.q).trim().endsWith('?')) warnings.push(`FAQ question should be a question: ${f.q}`);
        const w = words(f.a ?? '');
        if (w < 35 || w > 70) warnings.push(`FAQ answer is ${w} words (aim 40-60): ${f.q}`);
    }
    const answers = blocks(p.body, 'Answer');
    if (answers.length < 1 || answers.length > 2) warnings.push(`${answers.length} <Answer> blocks; use 1-2 under the main question headings`);
    for (const a of answers) { const w = words(a); if (w < 35 || w > 70) warnings.push(`<Answer> is ${w} words; aim for 40-60`); }
    const bodyWords = words(prose(p.body));
    if (bodyWords < 800 || bodyWords > 1800) warnings.push(`body is ${bodyWords} words; aim for 900-1,600`);
    const questions = headings(p.body).filter((h) => h.trim().endsWith('?'));
    if (questions.length < 2) warnings.push(`${questions.length} question-form headings; phrase at least two as searchers ask`);
    if (published.size >= 3 && ((d.related ?? []).length < 2 || (d.related ?? []).length > 4)) warnings.push('list 2-4 related posts');
    if (published.size >= 2 && !links(p.body).some((l) => l.startsWith('/blog/'))) warnings.push('no in-body link to another post');
    return { errors, warnings };
}

function checkTranslation(src, locale) {
    const errors = [], warnings = [];
    const t = readPost(src.slug, `${locale.code}.mdx`);
    if (!t) return { errors: [`missing translation ${locale.code}.mdx`], warnings };
    if (t.error) return { errors: [`${locale.code}.mdx: ${t.error}`], warnings };
    const tag = (s) => `${locale.code}: ${s}`;
    const d = t.data;
    for (const k of ['title', 'description']) if (!d[k]) errors.push(tag(`front matter needs '${k}'`));
    for (const k of ['date', 'updated', 'related', 'draft', 'image']) if (k in d) errors.push(tag(`'${k}' belongs in index.mdx only`));
    const want = sourceHash(src.raw);
    if (d.sourceHash !== want) errors.push(tag(`stale: sourceHash ${d.sourceHash ?? '(none)'} but index.mdx is ${want}; update the changed passages, then the hash`));
    if (!['draft', 'reviewed'].includes(d.translation)) errors.push(tag("translation must be 'draft' or 'reviewed' (review is optional)"));
    claims([prose(t.body), d.title, d.description, ...(d.faq ?? []).flatMap((f) => [f.q, f.a])].join(' '), errors, warnings);
    if ((d.faq ?? []).length !== (src.data.faq ?? []).length) errors.push(tag('FAQ entry count differs from the source'));
    for (const f of d.faq ?? []) if (/[`*[\]<>\n]/.test(f.a ?? '')) errors.push(tag(`FAQ answer must be one line of plain text: ${f.q}`));
    for (const kind of BLOCKS) if (blocks(t.body, kind).length !== blocks(src.body, kind).length) errors.push(tag(`<${kind}> count differs from the source`));
    const code = (b) => (b.match(/```[\s\S]*?```/g) ?? []);
    if (JSON.stringify(code(t.body)) !== JSON.stringify(code(src.body))) errors.push(tag('code blocks must match the source exactly'));
    if (headings(t.body).length !== headings(src.body).length) errors.push(tag('heading count differs from the source'));
    if (ctaCount(t.body) !== ctaCount(src.body)) errors.push(tag('<CTA> count differs from the source'));
    // The pool's translated articles use /blog/<locale>/, not /<locale>/blog/.
    const samePoolArticle = (url) => url
        .replace(`https://bntpool.com/blog/${locale.path}/`, 'https://bntpool.com/blog/')
        .replace(new RegExp(`^https://bntpool\\.com/stats\\?lang=${locale.path}$`), 'https://bntpool.com/stats');
    const ext = (b) => links(b).filter((l) => !l.startsWith('/')).map(samePoolArticle).sort().join(' ');
    if (ext(t.body) !== ext(src.body)) errors.push(tag('external links must match the source'));
    if (/^# /m.test(stripCode(t.body))) errors.push(tag('the body has an H1'));
    const tErrs = []; checkLinks(t, locale.path, tErrs); errors.push(...tErrs.map(tag));
    // Search results show about 30 CJK or Thai characters, or about 60 in other scripts.
    const titleBudget = ['ja', 'ko', 'th', 'zh-Hans', 'zh-Hant'].includes(locale.code) ? 32 : 65;
    if (d.seoTitle && [...d.seoTitle].length > titleBudget) warnings.push(tag(`seoTitle is ${[...d.seoTitle].length} characters; keep it under ${titleBudget}`));
    return { errors, warnings };
}

function grams(text, size = 8) {
    const tokens = text.toLowerCase().replace(/https?:\/\/\S+/g, '').match(/[a-z0-9]+/g) ?? [];
    const out = new Set();
    for (let i = 0; i + size <= tokens.length; i++) out.add(tokens.slice(i, i + size).join(' '));
    return out;
}

const selected = sources.filter((p) => !wanted.length || wanted.includes(p.slug));
const unknown = wanted.filter((w) => !sources.some((p) => p.slug === w));
if (unknown.length) { console.error(`unknown post(s): ${unknown.join(', ')}`); process.exit(1); }

if (overlapMode) {
    for (const p of selected) {
        const mine = grams(prose(p.body));
        // Skip posts that don't parse (for example one being written right now).
        const rows = sources.filter((o) => o.slug !== p.slug && o.body).map((o) => {
            const theirs = grams(prose(o.body));
            const shared = [...mine].filter((g) => theirs.has(g)).length;
            return { pct: (100 * shared) / Math.max(1, mine.size), shared, slug: o.slug, title: o.data?.title };
        }).sort((a, b) => b.pct - a.pct).slice(0, 3);
        console.log(`${p.slug}:`);
        for (const r of rows) console.log(`  ${r.pct.toFixed(1).padStart(5)}% of its 8-word phrases (${r.shared}) shared with ${r.slug} (${r.title})`);
        if (!rows.length) console.log('  no other posts');
    }
    process.exit(0);
}

let failures = 0;
for (const p of selected) {
    const result = p.error ? { errors: [p.error], warnings: [] } : checkSource(p);
    if (p.data && !p.data.draft) {
        for (const l of LOCALES.filter((x) => !onlyLocales.length || onlyLocales.includes(x.code))) {
            const r = checkTranslation(p, l);
            result.errors.push(...r.errors);
            result.warnings.push(...r.warnings);
        }
    }
    failures += result.errors.length + (strict ? result.warnings.length : 0);
    if (result.errors.length || result.warnings.length) {
        console.log(`${p.slug}${p.data?.draft ? ' (draft)' : ''}: ${result.errors.length} error(s), ${result.warnings.length} warning(s)`);
        for (const e of result.errors) console.log(`  error: ${e}`);
        for (const w of result.warnings) console.log(`  warn:  ${w}`);
    }
}
console.log(`checked ${selected.length} post(s): ${failures ? `${failures} failure(s)` : 'ok'}`);
process.exit(failures ? 1 : 0);
