// Scheduled availability probe — runs on a hosted CI cron.
// Sends a fixed request to a primary target via a CLI, classifies the outcome,
// and posts notifications to a private channel (out-of-band, so the thing being
// measured is never the thing that reports on it). Persists a compact state file.
//
// All targets, credentials, and identifiers come from CI secrets:
// SVC_KEY, TG_BOT_TOKEN, TG_CHAT_ID, PROBE_MIND, CONTROL_MIND.

import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { readFileSync, writeFileSync } from 'node:fs';

const exec = promisify(execFile);
// Primary and control targets are supplied via secrets, so the public repo
// never names or identifies what is being probed.
const MIND_ID = process.env.PROBE_MIND;
const ALIAS = 'gha-uptime';
// Control target: probed ONLY when the primary is silent, to tell "service
// down" apart from "this one target is wedged" — a control reply proves the
// service is up even when the primary is not answering.
const CONTROL_MIND_ID = process.env.CONTROL_MIND;
const CONTROL_ALIAS = 'gha-uptime-control';
// Fixed wording on purpose: a byte-identical request every run is cache-friendly
// upstream and keeps the reply minimal (replies are metered).
const PROMPT = 'Uptime check. Reply immediately with just: ok';
const PING_BUDGET_MS = 360_000; // observed reply times run up to ~6 min
// Must stay comfortably ABOVE the 120s blocking send timeout: when they were
// equal the control poll loop ran zero iterations and could only say "no reply".
const CONTROL_BUDGET_MS = 180_000;
const TG = process.env.TG_BOT_TOKEN;
const CHAT = process.env.TG_CHAT_ID;
const LOW_BALANCE = 5;
const now = () => new Date().toISOString();
const hhmm = (iso) => iso ? iso.slice(11, 16) + 'Z' : 'never';

async function cli(args, timeoutMs = 60_000) {
  const { stdout } = await exec('minds', args, { timeout: timeoutMs, maxBuffer: 8 * 1024 * 1024 });
  const parsed = JSON.parse(stdout);
  if (parsed.ok === false) throw new Error(parsed.message ?? `cli ${args[0]} failed`);
  return parsed;
}

async function tg(text, silent = false) {
  try {
    const r = await fetch(`https://api.telegram.org/bot${TG}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: CHAT, text, disable_notification: silent }),
      signal: AbortSignal.timeout(10_000),
    });
    if (!r.ok) console.log(`notify failed: HTTP ${r.status}`);
  } catch (e) {
    console.log(`notify failed: ${e.message}`);
  }
}

// Load the opaque persisted file ({s,a,o,f,h}) and expand into working fields.
// A missing/corrupt file must not crash the run before any alert can fire —
// a fresh clone simply starts from 'unknown'.
let raw = {};
try { raw = JSON.parse(readFileSync('state.json', 'utf8')); }
catch (e) { console.log(`state unreadable, starting fresh: ${String(e.message).slice(0, 120)}`); }
const state = {
  state: raw.s ?? raw.state ?? 'unknown',
  lastOkAt: raw.a ?? raw.lastOkAt ?? null,
  outageStartAt: raw.o ?? raw.outageStartAt ?? null,
  lowBalanceAlerted: raw.f ?? raw.lowBalanceAlerted ?? false,
  h: raw.h ?? [],
};

// ---- probes -------------------------------------------------------------
let healthOk = false, pingOk = false, latencyS = null, balance = null, netOk = true;

// --- history normalisation --------------------------------------------------
// The CLI's history contract is NOT stable across releases, and every mismatch
// here fails silently as "no reply" — which pages as an outage. v0.1.2 returned
// ONLY the Mind's messages, oldest-first, with the party in `partyType`.
// v0.1.3 (published 2026-07-21T09:16Z) returns BOTH sides, newest-first, and
// renamed the field to `senderType`. The rename alone took this monitor down:
// `filter(m => m.partyType === 0)` silently became `[]` on every poll, so the
// probe reported "no reply" for 19h while the agent was in fact answering
// every single time. Never key off one field name, and never assume an order.

/** A Mind (agent) message, across every known CLI shape. */
const isMindMsg = (m) => (m?.senderType ?? m?.partyType) === 0;

/** Newest Mind-reply timestamp in a page, epoch ms. 0 when there are none. */
function newestMindReplyAt(items) {
  let best = 0;
  for (const m of Array.isArray(items) ? items : []) {
    const t = Date.parse(m?.createdAt ?? '');
    if (isMindMsg(m) && Number.isFinite(t) && t > best) best = t;
  }
  return best;
}

/**
 * Read history and assert the response actually parses into the shape we rely
 * on. Returns { items, usable } — `usable` false means the CLI contract moved
 * under us again, which must be reported as a broken instrument, NOT downtime.
 */
async function readHistory(alias, limit = 200) {
  const h = await cli(['history', alias, '--limit', String(limit)], 30_000);
  const items = h.items ?? h.messages ?? [];
  // A conversation we just posted to is never legitimately empty, and every
  // item must carry a recognisable party field and a parseable timestamp.
  const usable = Array.isArray(items) && items.length > 0
    && items.every(m => (m?.senderType ?? m?.partyType) !== undefined && Number.isFinite(Date.parse(m?.createdAt ?? '')));
  return { items, usable };
}

// Send the request, then poll for a Mind reply strictly newer than a pre-send
// server-clock baseline. `--limit 200` is deliberate: it is the CLI maximum, so
// the newest message is inside the window under BOTH orderings (v0.1.2 returns
// the oldest N ascending, v0.1.3 the newest N descending) as long as the
// conversation stays under 200. Correlation scans the whole page and never
// relies on position, so ordering cannot break it.
async function pingMind(mindId, alias, budgetMs) {
  await cli(['chat', 'create', '--mind', mindId, '--alias', alias], 30_000);

  // Baseline BEFORE the send. A failure here must NOT silently degrade to 0 —
  // that would turn every pre-existing reply into a fake fresh one.
  let baseline = 0;
  let baselineKnown = false;
  try {
    const pre = await readHistory(alias);
    baseline = newestMindReplyAt(pre.items);
    baselineKnown = true;                 // an empty conversation is legitimately 0
  } catch (e) {
    console.log(`${alias}: baseline read failed: ${String(e.message).slice(0, 120)}`);
  }

  const t0 = Date.now();

  // Fire the send WITHOUT awaiting it. Awaiting a 120s blocking `--wait` inside
  // a 120s budget left the control probe exactly zero poll iterations, so it
  // could only ever return "no reply". The cursor poll below is the sole
  // detector; the wait is now just a (logged) side-channel.
  const sending = cli(['send', alias, PROMPT, '--wait', '--timeout', '120000'], 140_000)
    .catch(e => console.log(`${alias}: wait errored (polling anyway): ${String(e.message).slice(0, 120)}`));

  let sawUsableHistory = false;
  while (Date.now() - t0 < budgetMs) {
    await new Promise(r => setTimeout(r, 5_000));
    try {
      const { items, usable } = await readHistory(alias);
      if (usable) sawUsableHistory = true;
      else console.log(`${alias}: history unusable (${items.length} items) — CLI contract may have changed`);
      const newest = newestMindReplyAt(items);
      if (baselineKnown && newest > baseline) {
        return { ok: true, latencyS: Math.round((Date.now() - t0) / 1000) };
      }
    } catch (e) { console.log(`${alias}: poll error: ${String(e.message).slice(0, 120)}`); }
  }
  await sending.catch(() => {});

  // Distinguish "the agent stayed silent" from "we could not measure". Only the
  // former is downtime; conflating them is what produced 10 false outages.
  if (!baselineKnown || !sawUsableHistory) {
    return { ok: false, latencyS: null, broken: `${alias}: history never readable in ${Math.round(budgetMs / 1000)}s` };
  }
  return { ok: false, latencyS: null };
}

// One complete measurement round: health, primary probe, control probe,
// balance, classification, notification, persistence. Called repeatedly on a
// 12-minute grid by the driver at the foot of this file.
async function runRound() {
healthOk = false; pingOk = false; latencyS = null; balance = null; netOk = true;

try {
  const d = await cli(['doctor'], 30_000);
  const items = Array.isArray(d.checks) ? d.checks : Object.values(d.checks ?? {});
  // fail closed: unknown shape or any explicit ok:false = red
  healthOk = items.length > 0 && items.every(c => c?.ok !== false);
} catch (e) { healthOk = false; console.log(`health error: ${String(e.message).slice(0, 200)}`); }

let controlOk = null; // null = not needed (primary answered)
let brokenReason = null; // set when we could not MEASURE (never downtime)
try {
  const r = await pingMind(MIND_ID, ALIAS, PING_BUDGET_MS);
  pingOk = r.ok;
  latencyS = r.latencyS;
  if (r.broken) brokenReason = r.broken;
} catch (e) {
  brokenReason = `primary: ${String(e.message).slice(0, 120)}`;
  console.log(`primary error: ${String(e.message).slice(0, 200)}`);
}

// Primary silent: is the service down, or just this target? One control probe
// decides — and stops us paging for a single wedged target. Its budget must
// exceed the blocking send timeout, or it gets no poll iterations at all.
if (!pingOk) {
  try {
    const c = await pingMind(CONTROL_MIND_ID, CONTROL_ALIAS, CONTROL_BUDGET_MS);
    controlOk = c.ok;
    if (c.broken && !brokenReason) brokenReason = c.broken;
    console.log(`control: ${c.ok ? `ok (${c.latencyS}s)` : 'no reply'}`);
  } catch (e) {
    controlOk = false;
    if (!brokenReason) brokenReason = `control: ${String(e.message).slice(0, 120)}`;
    console.log(`control error: ${String(e.message).slice(0, 150)}`);
  }
}

try {
  const b = await cli(['cognition', 'balance', '--mind', MIND_ID], 30_000);
  balance = b.balance?.cognition ?? null;
} catch { /* balance unknown */ }

if (!pingOk && !healthOk) {
  // Tell a service problem apart from a local runner/network problem.
  try { await fetch('https://www.cloudflare.com', { method: 'HEAD', signal: AbortSignal.timeout(10_000) }); }
  catch { netOk = false; }
}

// ---- classify + notify --------------------------------------------------
const up = pingOk;
const prev = state.state;
// controlOk === true means another target answered fine, so the service is up
// and only the primary is wedged — a low-key note, never a page.
// `monitor-broken` outranks everything below it: if we could not READ the
// result, we know nothing about the service and must not claim an outage.
let kind = up ? 'up'
  : brokenReason ? 'monitor-broken'
  : !netOk ? 'runner-offline'
  : (healthOk && balance !== null && balance < LOW_BALANCE) ? 'low-balance'
  : controlOk === true ? 'primary-stuck'
  : 'svc-outage';

if (up) {
  await tg(`OK ${hhmm(now())} · reply ${latencyS}s · bal ${balance ?? '?'} · last OK ${hhmm(state.lastOkAt)} · [ci]`, true);
  // Announce recovery out of ANY unhealthy state, not just svc-outage —
  // otherwise a monitor-broken or primary-stuck spell ends in silence.
  if (prev !== 'up' && prev !== 'unknown') {
    const since = state.outageStartAt ?? state.lastOkAt;
    const mins = since ? Math.round((Date.now() - Date.parse(since)) / 60000) : '?';
    await tg(`✅ RECOVERED from ${prev} — responding again (reply ${latencyS}s) after ≈ ${mins} min (since ${hhmm(since)}). Bal ${balance ?? '?'}. [ci]`);
  }
  state.lastOkAt = now();
  state.outageStartAt = null;
  state.state = 'up';
} else if (kind === 'svc-outage') {
  if (prev !== 'svc-outage') {
    state.outageStartAt = now();
    await tg(`🔴 OUTAGE — primary probe failed (health ${healthOk ? 'ok' : 'FAIL'}). Last OK: ${hhmm(state.lastOkAt)}. Bal ${balance ?? '?'}. Next check ≤1h. [ci]`);
  } else {
    const mins = state.outageStartAt ? Math.round((Date.now() - Date.parse(state.outageStartAt)) / 60000) : '?';
    await tg(`🔴 still down — outage ongoing ≈ ${mins} min. Last OK: ${hhmm(state.lastOkAt)}. [ci]`);
  }
  state.state = 'svc-outage';
} else if (kind === 'monitor-broken') {
  // The instrument failed, not the service. Page once per transition so a
  // silently-blind monitor can never masquerade as 19h of clean "still down".
  if (prev !== 'monitor-broken') {
    await tg(`🛠 MONITOR BROKEN — cannot measure, so this is NOT an outage claim. ${brokenReason}. Check the CLI contract (a release can rename history fields). Last OK: ${hhmm(state.lastOkAt)}. [ci]`);
  }
  state.state = 'monitor-broken';
} else if (kind === 'primary-stuck') {
  // Service proven up by the control probe — inform, don't page.
  if (prev !== 'primary-stuck') {
    await tg(`🟡 primary not replying within ${PING_BUDGET_MS / 60000} min, but service is UP (control answered). Likely the primary is wedged/slow, not an outage. Last OK: ${hhmm(state.lastOkAt)}. Bal ${balance ?? '?'}. [ci]`, true);
  }
  state.state = 'primary-stuck';
} else if (kind === 'low-balance') {
  if (!state.lowBalanceAlerted) {
    await tg(`🟠 TOP UP — primary balance is ${balance} (<${LOW_BALANCE}); probes can't run. Service itself looks up (health ok). [ci]`);
    state.lowBalanceAlerted = true;
  }
  state.state = 'low-balance';
} else {
  console.log('runner network appears offline — not counted as service downtime');
  state.state = 'runner-offline';
}
if (balance !== null && balance >= LOW_BALANCE * 1.2) state.lowBalanceAlerted = false; // hysteresis re-arm

// ---- persist ------------------------------------------------------------
// Public file carries only unlabeled number-triples: [epoch, code, value].
// No balance, no field names, no units.
// 6 = monitor-broken: an UNMEASURED slot. It must never be counted as a
// failure of the service — the 10 rows this file recorded as code 4 on
// 2026-07-21/22 were all of this kind, and the agent replied to every one.
const CODE = { up: 0, 'primary-stuck': 2, 'low-balance': 3, 'svc-outage': 4, 'runner-offline': 5, 'monitor-broken': 6 };
const code = up ? (latencyS != null && latencyS > 60 ? 1 : 0) : (CODE[kind] ?? 4);
state.h = (state.h ?? []).concat([[Math.floor(Date.now() / 1000), code, latencyS ?? 0]]);
if (state.h.length > 4000) state.h = state.h.slice(-4000);
writeFileSync('state.json', JSON.stringify({ s: state.state, a: state.lastOkAt, o: state.outageStartAt, f: state.lowBalanceAlerted, h: state.h }));
console.log(`result: ${kind}${latencyS != null ? ` (${latencyS}s)` : ''} bal=${balance}`); // stdout only, not committed
}

// ---- driver: a 12-minute grid inside ONE job ------------------------------
// GitHub throttles high-frequency `schedule` triggers hard: this workflow asks
// for hourly and actually landed every 2-3 hours, with the fire minute drifting
// by up to 50 minutes. Rather than fight the scheduler, one triggered job now
// stays alive and probes on a 12-minute WALL-CLOCK grid. That restores true
// 12-minute resolution, keeps slots aligned across jobs, and is immune to
// trigger jitter — and Actions minutes are free on a public repo.
const GRID_MS = 12 * 60_000;
const JOB_BUDGET_MS = 50 * 60_000;   // finish before the next hourly trigger
const startedAt = Date.now();

for (let round = 1; ; round++) {
  console.log(`--- round ${round} (t+${Math.round((Date.now() - startedAt) / 60000)}m) ---`);
  // A thrown round must never kill the job: the remaining slots are still
  // worth measuring, and an unhandled rejection would look like a green run.
  try { await runRound(); }
  catch (e) { console.log(`round ${round} threw: ${String(e.message).slice(0, 200)}`); }

  const nextSlot = Math.ceil((Date.now() + 1_000) / GRID_MS) * GRID_MS;
  if (nextSlot - startedAt > JOB_BUDGET_MS) break;
  const wait = nextSlot - Date.now();
  if (wait > 0) await new Promise(r => setTimeout(r, wait));
}
console.log(`job complete after ${Math.round((Date.now() - startedAt) / 60000)} min`);
