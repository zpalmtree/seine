# Writing guide (brief for any writing agent)

You're writing a post for the seineminer.com blog, which covers Blocknet in general as well as
mining it. Blocknet is a privacy cryptocurrency you can pay by name, with a memory-hard Argon2id
proof of work; seine is the free, open-source (BSD-3) miner for it, and bntpool.com is the pool.
The blog brings people searching about Blocknet, its features, mining and Argon2id to the site,
and from there to seine and bntpool.com. Coin topics (privacy, payment by name, memos, the
wallet, the Faraday upgrade) are as welcome as mining ones, and every post links to the miner
and the pool where it fits. Follow this file, [`README.md`](README.md) (workflow, post format,
translations) and [`fact-sheet.md`](fact-sheet.md) exactly.

## The first rule: promote, never criticize

Everything we publish promotes **Blocknet, bntpool.com and seine**. Nothing negative about any
of them, the same policy as the Sol Incinerator blog.

- Lead with strengths and build the page around them: seine's best-per-generation results,
  auto-tuning, CPU and GPU mining together, zero setup; bntpool.com as the easy default pool;
  Blocknet's privacy by default, payment by name and fair launch.
- No downsides, limitations, risk sections, pros-and-cons lists, "is it worth it" framing,
  unflattering caveats, weak hardware results, or comparisons where we look worse. Don't
  volunteer fees. State a limitation only when the reader can't complete the task without it
  (for example, which archive has GPU support), and say it as a plain fact without apology.
- Comparisons with other miners, pools or coins lead with our advantages and never disparage
  the other side. Leave out any axis where they win.
- Never state anything false, unsourced or misleading. A checkable falsehood costs more
  credibility than any claim earns, so omit a weak point rather than misstate it.

`npm run check -- <slug> --strict` warns on negative phrasing ("experimental", "drawback",
"unfortunately", "not recommended" and similar); rewrite or remove each hit.

## Before you write

- Read `keyword-map.md`. Your post owns one search job that no other URL owns. Put the
  one-sentence intent statement from README step 1 in your batch notes.
- Read `src/content/blog/how-to-mine-blocknet/index.mdx` for tone and component use.
- Every number, fee, date, hardware result and protocol parameter comes from the fact sheet or
  from your batch's `research.md` with a source. If a fact isn't there, find a source and add
  it, or write around it. Arithmetic on sourced numbers is fine ("8 threads × 2 GB = 16 GB");
  invented "typical user" statistics are not.

## Assume the reader knows Blocknet

Readers arrive already knowing what Blocknet is. Don't explain which Blocknet this is, add
"this is not the BLOCK project" notes, or define the coin in an intro; start with the task.
One naming rule still holds: never link blocknet.org (an unrelated project). The `$BNT`
cashtag is welcome; we are reclaiming it from Bancor (owner decision, 2026-11-05).

## Claims you must not make

From `marketing/brand/taglines.md`, the owner's decisions and the fact sheet. `npm run check`
fails the build on most of them.

- No "untraceable" or "anonymous" (匿名, 不可追踪); say "private".
- No "most private coin" or "more private than Monero".
- Post-quantum: today's chain uses classical elliptic-curve cryptography, so never call it
  post-quantum. You may describe the Faraday upgrade's planned post-quantum spend proofs,
  attributed to blocknetcrypto.com and in the future tense.
- No profit promises, "passive income" or "risk-free". Profitability depends on coin price,
  network difficulty and electricity cost; give the reader the arithmetic and a way to measure.
- No unannounced Faraday details. Only what blocknetcrypto.com states publicly.
- No Windows ARM64 or production Metal support claims. AMD GPU support shipped in v0.3.0; say which cards (RX 6000, RX 7000, RX 9000, Radeon PRO).
- seine's term is **threads** (the `--threads` flag), never "workers". On the pool, a "worker
  name" is fine because that's what the pool calls it.
- Don't lead with fees. Mention seine's dev fee (flat 2% on every pool and in solo mode) only where
  a reader can't act without it.

## House style

- Plain and direct, second person. Describe what the software does. No marketing
  constructions: no "X, not Y" ("measured, not promised"), no taglines ("built to…", "every
  hash", "start today"), no hype, no emoji, no filler intros. The first paragraph says what
  the reader will be able to do.
- American English. Sentence-case headings.
- Hardware: names people know in tables ("AMD EPYC · Zen 5", "Apple M4 Max") with the exact
  chip alongside. Show the best measured result per generation and leave out weak showings.
- Commands in fenced code blocks; flags, file names and addresses in backticks.
- Explain why, not just what: why a thread needs 2 GB, why huge pages help. That's what makes
  a post worth ranking over a forum thread.
- Original evidence wins: our benchmarks, a dashboard screenshot, a measured before/after.
  Say how and when it was measured.
- Close with one `<CTA>` that fits the post: a seine download for mining topics, bntpool.com for
  payouts or names, with a title and one sentence about what the reader gets (see README).
- Link to seine's docs on GitHub for deep reference (performance tables, huge pages, large
  servers) instead of copying them.
- Chinese translations follow the same rules and stay plain and literal.

## Finish line

1. `npm run check -- <slug> --strict` passes.
2. `npm run overlap -- <slug>`: the closest posts are reviewed and the boundary is recorded in
   `keyword-map.md`.
3. Every factual claim traces to the fact sheet or batch research.
4. The post stays `draft: true` until the user approves it; translation and publishing are
   README steps 6-7.
