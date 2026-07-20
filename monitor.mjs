// Minds platform uptime monitor — runs on GitHub Actions (hourly cron).
// Deep-pings the test Mind via the official minds CLI, classifies failures,
// alerts Po via his own Telegram bot (never via Minds — a downed platform
// can't self-report), and persists state.json back to the repo.
//
// Secrets (Actions): MINDS_BUILDER_API_KEY, TG_BOT_TOKEN, TG_CHAT_ID.
// Design reviewed by LLM council 2026-07-20: silent OK messages, audible
// state changes, low-balance alert with hysteresis, no Mind text rendered.

import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { readFileSync, writeFileSync } from 'node:fs';

const exec = promisify(execFile);
// Probe target and control are supplied via secrets so the public repo never
// names or identifies which Minds are being monitored.
const MIND_ID = process.env.PROBE_MIND;
const ALIAS = 'gha-uptime';
// Control target: pinged ONLY when the primary fails, to tell "platform down"
// apart from "this one Mind is wedged/slow" (a control reply proves the
// platform is serving even when the primary is silent).
const CONTROL_MIND_ID = process.env.CONTROL_MIND;
const CONTROL_ALIAS = 'gha-uptime-control';
// Fixed wording on purpose: an identical prompt every run is cache-friendly
// upstream and keeps the reply minimal (cognition is billed per reply).
const PROMPT = 'Uptime check. Reply immediately with just: ok';
const PING_BUDGET_MS = 360_000; // testing.po has been observed at ~6 min
const TG = process.env.TG_BOT_TOKEN;
const CHAT = process.env.TG_CHAT_ID;
const LOW_BALANCE = 5;
const now = () => new Date().toISOString();
const hhmm = (iso) => iso ? iso.slice(11, 16) + 'Z' : 'never';

async function cli(args, timeoutMs = 60_000) {
  const { stdout } = await exec('minds', args, { timeout: timeoutMs, maxBuffer: 8 * 1024 * 1024 });
  const parsed = JSON.parse(stdout);
  if (parsed.ok === false) throw new Error(parsed.message ?? `minds ${args[0]} failed`);
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
    if (!r.ok) console.log(`tg send failed: HTTP ${r.status}`);
  } catch (e) {
    console.log(`tg send failed: ${e.message}`);
  }
}

// Load the opaque persisted file ({s,a,o,f,h}) and expand into working fields.
// Tolerates the pre-obfuscation shape too, for a clean one-time migration.
const raw = JSON.parse(readFileSync('state.json', 'utf8'));
const state = {
  state: raw.s ?? raw.state ?? 'unknown',
  lastOkAt: raw.a ?? raw.lastOkAt ?? null,
  outageStartAt: raw.o ?? raw.outageStartAt ?? null,
  lowBalanceAlerted: raw.f ?? raw.lowBalanceAlerted ?? false,
  h: raw.h ?? [],
};

// ---- probes -------------------------------------------------------------
let doctorOk = false, pingOk = false, latencyS = null, balance = null, netOk = true;

try {
  const d = await cli(['doctor'], 30_000);
  const items = Array.isArray(d.checks) ? d.checks : Object.values(d.checks ?? {});
  // fail closed: unknown shape or any explicit ok:false = red
  doctorOk = items.length > 0 && items.every(c => c?.ok !== false);
} catch (e) { doctorOk = false; console.log(`doctor error: ${String(e.message).slice(0, 200)}`); }

// Deep ping. The CLI's `send --wait` streaming is flaky (errors even when the
// reply lands — observed both locally and on GHA runners), so the send is only
// the trigger; the source of truth is polling `history` for a reply that
// arrives after our probe token.
// Ping a Mind: send, then poll history for a reply newer than a server-clock
// baseline. `send --wait` is unreliable (times out while the reply lands), and
// `history` returns only the Mind's own messages, so baseline+poll is the only
// correlation that actually works.
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
    console.log(`${alias}: send --wait errored (polling history anyway): ${String(e.message).slice(0, 120)}`);
  }
  while (Date.now() - t0 < budgetMs) {
    try {
      const h = await cli(['history', alias, '--limit', '10'], 30_000);
      const replies = (h.items ?? h.messages ?? []).filter(m => m.partyType === 0);
      if (replies.some(m => Date.parse(m.createdAt) > baseline)) {
        return { ok: true, latencyS: Math.round((Date.now() - t0) / 1000) };
      }
    } catch (e) { console.log(`${alias}: history poll error: ${String(e.message).slice(0, 120)}`); }
    await new Promise(r => setTimeout(r, 10_000));
  }
  return { ok: false, latencyS: null };
}

let controlOk = null; // null = not needed (primary answered)
try {
  const r = await pingMind(MIND_ID, ALIAS, PING_BUDGET_MS);
  pingOk = r.ok;
  latencyS = r.latencyS;
} catch (e) { console.log(`ping setup error: ${String(e.message).slice(0, 200)}`); }

// Primary silent: is the PLATFORM down, or just this Mind? One control ping
// decides — and stops us paging Po for a wedged test Mind.
if (!pingOk) {
  try {
    const c = await pingMind(CONTROL_MIND_ID, CONTROL_ALIAS, 120_000);
    controlOk = c.ok;
    console.log(`control ping: ${c.ok ? `ok (${c.latencyS}s)` : 'no reply'}`);
  } catch (e) { controlOk = false; console.log(`control ping error: ${String(e.message).slice(0, 150)}`); }
}

try {
  const b = await cli(['cognition', 'balance', '--mind', MIND_ID], 30_000);
  balance = b.balance?.cognition ?? null;
} catch { /* balance unknown */ }

if (!pingOk && !doctorOk) {
  // Distinguish "Minds down" from "runner/network problem"
  try { await fetch('https://www.cloudflare.com', { method: 'HEAD', signal: AbortSignal.timeout(10_000) }); }
  catch { netOk = false; }
}

// ---- classify + alert ---------------------------------------------------
const up = pingOk;
const prev = state.state;
// controlOk === true means another Mind answered fine, so the platform is up
// and only our test Mind is wedged — a "mind-stuck" note, never an outage page.
let kind = up ? 'up'
  : !netOk ? 'runner-offline'
  : (doctorOk && balance !== null && balance < LOW_BALANCE) ? 'low-balance'
  : controlOk === true ? 'mind-stuck'
  : 'minds-outage';

if (up) {
  const line = `OK ${hhmm(now())} · reply ${latencyS}s · balance ${balance ?? '?'} · last OK ${hhmm(state.lastOkAt)} · [gha]`;
  await tg(line, true); // silent per-ping log, per Po's spec
  if (prev === 'minds-outage') {
    const mins = state.outageStartAt ? Math.round((Date.now() - Date.parse(state.outageStartAt)) / 60000) : '?';
    await tg(`✅ RECOVERED — Minds is responding again (reply ${latencyS}s). Downtime ≈ ${mins} min (since ${hhmm(state.outageStartAt)}). Balance ${balance ?? '?'}. [gha]`);
  }
  state.lastOkAt = now();
  state.outageStartAt = null;
  state.state = 'up';
} else if (kind === 'minds-outage') {
  if (prev !== 'minds-outage') {
    state.outageStartAt = now();
    await tg(`🔴 MINDS OUTAGE — deep ping to test Mind failed (doctor ${doctorOk ? 'ok' : 'FAIL'}). Last OK: ${hhmm(state.lastOkAt)}. Balance ${balance ?? '?'}. Next check ≤1h (GitHub cron). [gha]`);
  } else {
    const mins = state.outageStartAt ? Math.round((Date.now() - Date.parse(state.outageStartAt)) / 60000) : '?';
    await tg(`🔴 still down — outage ongoing ≈ ${mins} min. Last OK: ${hhmm(state.lastOkAt)}. [gha]`);
  }
  state.state = 'minds-outage';
} else if (kind === 'mind-stuck') {
  // Platform proven up by the control Mind — inform, don't page.
  if (prev !== 'mind-stuck') {
    await tg(`🟡 testing.po not replying within ${PING_BUDGET_MS / 60000} min, but the platform is UP (a control Mind answered). Likely that Mind is wedged/slow, not an outage. Last OK: ${hhmm(state.lastOkAt)}. Balance ${balance ?? '?'}. [gha]`, true);
  }
  state.state = 'mind-stuck';
} else if (kind === 'low-balance') {
  if (!state.lowBalanceAlerted) {
    await tg(`🟠 TOP UP — testing.po balance is ${balance} (<${LOW_BALANCE}); pings can't run. Platform itself looks up (doctor ok). [gha]`);
    state.lowBalanceAlerted = true;
  }
  state.state = 'low-balance';
} else {
  console.log('runner network appears offline — not counting as Minds downtime');
  state.state = 'runner-offline';
}
if (balance !== null && balance >= LOW_BALANCE * 1.2) state.lowBalanceAlerted = false; // hysteresis re-arm

// ---- persist ------------------------------------------------------------
// Public file carries only unlabeled number-triples [epoch, statusCode, value].
// No balance (private — Telegram only), no field names, no units. Codes map
// up=0 slow=1 stuck=2 lowbal=3 outage=4 offline=5.
const CODE = { up: 0, 'mind-stuck': 2, 'low-balance': 3, 'minds-outage': 4, 'runner-offline': 5 };
const code = up ? (latencyS != null && latencyS > 60 ? 1 : 0) : (CODE[kind] ?? 4);
state.h = (state.h ?? []).concat([[Math.floor(Date.now() / 1000), code, latencyS ?? 0]]);
if (state.h.length > 4000) state.h = state.h.slice(-4000);
delete state.history; // drop any legacy labeled history + balance
writeFileSync('state.json', JSON.stringify({ s: state.state, a: state.lastOkAt, o: state.outageStartAt, f: state.lowBalanceAlerted, h: state.h }));
console.log(`result: ${kind}${latencyS ? ` (${latencyS}s)` : ''} balance=${balance}`); // stdout only, not committed
