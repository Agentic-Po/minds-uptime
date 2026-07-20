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
const raw = JSON.parse(readFileSync('state.json', 'utf8'));
const state = {
  state: raw.s ?? raw.state ?? 'unknown',
  lastOkAt: raw.a ?? raw.lastOkAt ?? null,
  outageStartAt: raw.o ?? raw.outageStartAt ?? null,
  lowBalanceAlerted: raw.f ?? raw.lowBalanceAlerted ?? false,
  h: raw.h ?? [],
};

// ---- probes -------------------------------------------------------------
let healthOk = false, pingOk = false, latencyS = null, balance = null, netOk = true;

try {
  const d = await cli(['doctor'], 30_000);
  const items = Array.isArray(d.checks) ? d.checks : Object.values(d.checks ?? {});
  // fail closed: unknown shape or any explicit ok:false = red
  healthOk = items.length > 0 && items.every(c => c?.ok !== false);
} catch (e) { healthOk = false; console.log(`health error: ${String(e.message).slice(0, 200)}`); }

// Send the request, then poll for a reply newer than a pre-send server-clock
// baseline. The CLI's synchronous wait is unreliable (errors while the reply
// still lands), and history lists only the target's own messages, so
// baseline + poll is the only correlation that works.
async function pingMind(mindId, alias, budgetMs) {
  await cli(['chat', 'create', '--mind', mindId, '--alias', alias], 30_000);
  let baseline = 0;
  try {
    const pre = await cli(['history', alias, '--limit', '5'], 30_000);
    baseline = Math.max(0, ...(pre.items ?? []).filter(m => m.partyType === 0).map(m => Date.parse(m.createdAt) || 0));
  } catch { /* empty conversation is fine */ }
  const t0 = Date.now();
  try {
    await cli(['send', alias, PROMPT, '--wait', '--timeout', '120000'], 140_000);
  } catch (e) {
    console.log(`${alias}: wait errored (polling anyway): ${String(e.message).slice(0, 120)}`);
  }
  while (Date.now() - t0 < budgetMs) {
    try {
      const h = await cli(['history', alias, '--limit', '10'], 30_000);
      const replies = (h.items ?? h.messages ?? []).filter(m => m.partyType === 0);
      if (replies.some(m => Date.parse(m.createdAt) > baseline)) {
        return { ok: true, latencyS: Math.round((Date.now() - t0) / 1000) };
      }
    } catch (e) { console.log(`${alias}: poll error: ${String(e.message).slice(0, 120)}`); }
    await new Promise(r => setTimeout(r, 10_000));
  }
  return { ok: false, latencyS: null };
}

let controlOk = null; // null = not needed (primary answered)
try {
  const r = await pingMind(MIND_ID, ALIAS, PING_BUDGET_MS);
  pingOk = r.ok;
  latencyS = r.latencyS;
} catch (e) { console.log(`primary error: ${String(e.message).slice(0, 200)}`); }

// Primary silent: is the service down, or just this target? One control probe
// decides — and stops us paging for a single wedged target.
if (!pingOk) {
  try {
    const c = await pingMind(CONTROL_MIND_ID, CONTROL_ALIAS, 120_000);
    controlOk = c.ok;
    console.log(`control: ${c.ok ? `ok (${c.latencyS}s)` : 'no reply'}`);
  } catch (e) { controlOk = false; console.log(`control error: ${String(e.message).slice(0, 150)}`); }
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
let kind = up ? 'up'
  : !netOk ? 'runner-offline'
  : (healthOk && balance !== null && balance < LOW_BALANCE) ? 'low-balance'
  : controlOk === true ? 'primary-stuck'
  : 'svc-outage';

if (up) {
  await tg(`OK ${hhmm(now())} · reply ${latencyS}s · bal ${balance ?? '?'} · last OK ${hhmm(state.lastOkAt)} · [ci]`, true);
  if (prev === 'svc-outage') {
    const mins = state.outageStartAt ? Math.round((Date.now() - Date.parse(state.outageStartAt)) / 60000) : '?';
    await tg(`✅ RECOVERED — responding again (reply ${latencyS}s). Down ≈ ${mins} min (since ${hhmm(state.outageStartAt)}). Bal ${balance ?? '?'}. [ci]`);
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
const CODE = { up: 0, 'primary-stuck': 2, 'low-balance': 3, 'svc-outage': 4, 'runner-offline': 5 };
const code = up ? (latencyS != null && latencyS > 60 ? 1 : 0) : (CODE[kind] ?? 4);
state.h = (state.h ?? []).concat([[Math.floor(Date.now() / 1000), code, latencyS ?? 0]]);
if (state.h.length > 4000) state.h = state.h.slice(-4000);
writeFileSync('state.json', JSON.stringify({ s: state.state, a: state.lastOkAt, o: state.outageStartAt, f: state.lowBalanceAlerted, h: state.h }));
console.log(`result: ${kind}${latencyS ? ` (${latencyS}s)` : ''} bal=${balance}`); // stdout only, not committed
