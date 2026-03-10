// =============================================================================
// static/db_dashboard.js
// snapcoin-db-inspector/static/db_dashboard.js
// v0.4.0-wallet-intelligence.1
// Dashboard JS: WebSocket client + HTTP orchestrator + DOM renderers.
//
// Change (v0.4.0 ... .1):
// - FULL REWRITE for new sidebar-free layout
// - ADD global intelligence panel — fetches /api/wallet_intelligence on connect,
//   populates 5 clickable chips (most active, largest output, most fragmented,
//   fastest growing, highest spend rate). Each chip loads address into inspector.
// - ADD topbar globals — fetches /api/globals on connect, populates metrics strip.
// - ADD inspect wallet orchestrator — single entry point that chains:
//     1. WS utxos_for  -> get txid list
//     2. /api/wallet_stats/:address (HTTP) -> per-wallet summary strip
//     3. parallel /api/decode/:txid (HTTP) for all txids -> timeline rows
//        rendered as each response arrives (streaming UX)
// - ADD UTXO timeline renderer — each tx row fully expanded inline
// - ADD filter bar — ALL / UNSPENT / SPENT, updates visible rows live
// - ADD sort toggle — reverses timeline render order
// - ADD decode progress bar — updates as parallel fetches complete
// - KEEP top receivers renderer (now renders into view-top)
// - KEEP pie chart builder
// - KEEP WebSocket client + reconnect logic
// - REMOVE sidebar activity log, txid input, Default Tree wiring
//
// Atomic divisor: 100_000_000 (8 decimals)
// API flows:
//   WS  -> globals, utxos_for, top_receivers
//   HTTP-> /api/wallet_intelligence, /api/wallet_stats/:addr, /api/decode/:txid
// =============================================================================

(function () {
'use strict';

// =============================================================================
// SNAP conversion helpers
// =============================================================================

const SNAP_DIVISOR  = 100_000_000n;
const SNAP_DECIMALS = 8;

function toSnap(atomic) {
  if (atomic == null) return '—';
  const n = BigInt(Math.round(Number(atomic)));
  const whole = n / SNAP_DIVISOR;
  const frac  = (n % SNAP_DIVISOR).toString().padStart(SNAP_DECIMALS, '0');
  const trim  = frac.replace(/0+$/, '').padEnd(2, '0');
  return whole.toLocaleString() + '.' + trim;
}

function toSnapFull(atomic) {
  if (atomic == null) return '—';
  const n = BigInt(Math.round(Number(atomic)));
  const whole = n / SNAP_DIVISOR;
  const frac  = (n % SNAP_DIVISOR).toString().padStart(SNAP_DECIMALS, '0');
  return whole.toLocaleString() + '.' + frac;
}

function atomicFmt(v) {
  return v != null ? Number(v).toLocaleString() : '—';
}

function shortAddr(a, len) {
  len = len || 20;
  if (!a) return '—';
  return a.length > len ? a.slice(0, len) + '…' : a;
}

function numFmt(v) {
  return v != null ? Number(v).toLocaleString() : '—';
}

// =============================================================================
// DOM helpers
// =============================================================================

const $ = id => document.getElementById(id);

function el(tag, attrs, children) {
  const n = document.createElement(tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (v == null) continue;
      if (k === 'class')      n.className = v;
      else if (k === 'text')  n.textContent = String(v);
      else if (k === 'style') n.setAttribute('style', String(v));
      else if (k.startsWith('on') && typeof v === 'function')
        n.addEventListener(k.slice(2).toLowerCase(), v);
      else n.setAttribute(k, String(v));
    }
  }
  if (children != null) {
    const arr = Array.isArray(children) ? children : [children];
    for (const c of arr) {
      if (c == null) continue;
      if (typeof c === 'string') n.appendChild(document.createTextNode(c));
      else n.appendChild(c);
    }
  }
  return n;
}

function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

// =============================================================================
// View switching — view-empty / view-wallet / view-top / view-raw
// =============================================================================

function showView(name) {
  document.querySelectorAll('.view').forEach(v => {
    v.classList.toggle('on', v.id === 'view-' + name);
  });
}

// =============================================================================
// Raw JSON panel
// =============================================================================

function setRaw(data, meta) {
  const out = $('rawOut');
  const mm  = $('rawMeta');
  if (out) out.textContent = typeof data === 'string'
    ? data
    : JSON.stringify(data, null, 2);
  if (mm) mm.textContent = meta || '';
}

// =============================================================================
// WebSocket
// =============================================================================

let ws       = null;
let wsLive   = false;
const wsQueue = [];

function wsState(live, text) {
  wsLive = !!live;
  const dot = $('wsDot');
  const txt = $('wsTxt');
  if (dot) dot.className = 'ws-dot' + (live ? ' live' : '');
  if (txt) txt.textContent = text;
}

function connect() {
  if (ws && ws.readyState < 2) { try { ws.close(); } catch (_) {} }

  try {
    ws = new WebSocket(
      (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws'
    );
  } catch (e) {
    wsState(false, 'failed');
    return;
  }

  wsState(false, 'connecting…');

  ws.onopen = () => {
    wsState(true, 'live');
    flushWsQueue();
    // Auto-load globals and intelligence on connect
    wsSend({ cmd: 'globals' });
    fetchIntelligence();
  };

  ws.onclose = () => wsState(false, 'disconnected');
  ws.onerror = () => wsState(false, 'error');

  ws.onmessage = ev => {
    try { onWsMessage(JSON.parse(ev.data)); }
    catch (e) { setRaw(ev.data, 'parse error'); }
  };
}

function flushWsQueue() {
  if (!ws || ws.readyState !== 1) return;
  while (wsQueue.length) {
    const obj = wsQueue.shift();
    try { ws.send(JSON.stringify(obj)); }
    catch (e) { wsQueue.unshift(obj); break; }
  }
}

function wsSend(obj) {
  if (ws && ws.readyState === 1) {
    try { ws.send(JSON.stringify(obj)); return; }
    catch (e) {}
  }
  wsQueue.push(obj);
  connect();
}

// =============================================================================
// WS message dispatch
// =============================================================================

function onWsMessage(msg) {
  setRaw(msg, msg.cmd + ' · ' + (msg.ok ? 'ok' : 'err'));
  if (!msg.ok) return;

  switch (msg.cmd) {
    case 'globals':       onGlobals(msg.data);       break;
    case 'utxos_for':     onUtxosFor(msg.data);      break;
    case 'top_receivers': renderTopReceivers(msg.data); break;
  }
}

// =============================================================================
// Globals — populates topbar metrics strip
// =============================================================================

function onGlobals(g) {
  const set = (id, v) => { const e = $(id); if (e) e.textContent = v; };
  set('gWallets',     numFmt(g.wallets_with_unspent));
  set('gTxids',       numFmt(g.tx_entries_scanned));
  set('gOutputs',     numFmt(g.outputs_total));
  set('gUnspent',     numFmt(g.outputs_unspent));
  set('gCirculation', toSnap(g.utxo_total_unspent_atomic) + ' SNAP');
}

// =============================================================================
// Intelligence panel — HTTP fetch, chips populated on arrival
// =============================================================================

async function fetchIntelligence() {
  try {
    const r = await fetch('/api/wallet_intelligence');
    if (!r.ok) throw new Error(r.statusText);
    const d = await r.json();
    renderIntelligence(d);
  } catch (e) {
    const loading = $('intelLoading');
    if (loading) loading.textContent = 'intelligence unavailable';
  }
}

function renderIntelligence(d) {
  const loading = $('intelLoading');
  const chips   = $('intelChips');
  if (loading) loading.style.display = 'none';
  if (chips)   chips.style.display   = 'flex';

  // Most active
  const icActive = $('ichip-active');
  if (icActive) {
    $('ic-active-val').textContent = shortAddr(d.most_active_address, 18);
    $('ic-active-sub').textContent = numFmt(d.most_active_tx_count) + ' txs';
    icActive.title = d.most_active_address;
    icActive.onclick = () => inspectWallet(d.most_active_address);
  }

  // Largest output
  const icLargest = $('ichip-largest');
  if (icLargest) {
    $('ic-largest-val').textContent = shortAddr(d.largest_output_receiver, 18);
    $('ic-largest-sub').textContent = d.largest_output_amount_snap + ' SNAP';
    icLargest.title = d.largest_output_receiver;
    icLargest.onclick = () => inspectWallet(d.largest_output_receiver);
  }

  // Most fragmented
  const icFrag = $('ichip-frag');
  if (icFrag) {
    $('ic-frag-val').textContent = shortAddr(d.most_fragmented_address, 18);
    $('ic-frag-sub').textContent = numFmt(d.most_fragmented_unspent_count) + ' unspent outputs';
    icFrag.title = d.most_fragmented_address;
    icFrag.onclick = () => inspectWallet(d.most_fragmented_address);
  }

  // Fastest growing
  const icGrowing = $('ichip-growing');
  if (icGrowing) {
    $('ic-growing-val').textContent = shortAddr(d.fastest_growing_address, 18);
    $('ic-growing-sub').textContent = d.fastest_growing_delta_snap + ' SNAP delta';
    icGrowing.title = d.fastest_growing_address;
    icGrowing.onclick = () => inspectWallet(d.fastest_growing_address);
  }

  // Highest spend rate
  const icSpend = $('ichip-spend');
  if (icSpend) {
    $('ic-spend-val').textContent = shortAddr(d.highest_spend_rate_address, 18);
    $('ic-spend-sub').textContent = d.highest_spend_rate_pct.toFixed(1) + '% spent'
      + ' (' + numFmt(d.highest_spend_rate_spent) + '/' + numFmt(d.highest_spend_rate_total) + ')';
    icSpend.title = d.highest_spend_rate_address;
    icSpend.onclick = () => inspectWallet(d.highest_spend_rate_address);
  }
}

// =============================================================================
// Inspect wallet — main orchestrator
// Chain: utxos_for (WS) -> wallet_stats (HTTP) + parallel decode (HTTP)
// =============================================================================

// State for current inspection
let currentAddress  = '';
let currentTxids    = [];
let currentDecoded  = [];  // { txid, outputs, totalUnspent, unspentCount, spentCount }
let decodeCompleted = 0;
let sortNewest      = true;
let activeFilter    = 'all';

function inspectWallet(address) {
  if (!address) return;
  address = address.trim();
  if (!address) return;

  // Update search input
  const inp = $('addr');
  if (inp) inp.value = address;

  currentAddress  = address;
  currentTxids    = [];
  currentDecoded  = [];
  decodeCompleted = 0;

  // Reset wallet view
  const summary  = $('walletSummary');
  const progress = $('decodeProgress');
  const filterB  = $('filterBar');
  const timeline = $('timeline');

  if (summary)  summary.style.display  = 'none';
  if (progress) progress.style.display = 'flex';
  if (filterB)  filterB.style.display  = 'none';
  if (timeline) clear(timeline);

  // Show loading placeholder
  if (progress) {
    $('dpBar').style.width = '0%';
    $('dpText').textContent = 'fetching transactions…';
  }

  showView('wallet');

  // Fire wallet_stats in parallel — doesn't depend on utxos_for
  fetchWalletStats(address);

  // Get txid list via WS
  wsSend({ cmd: 'utxos_for', address, limit: 500 });
}

// Called when WS returns utxos_for
async function onUtxosFor(txids) {
  if (!Array.isArray(txids)) return;

  currentTxids    = txids;
  decodeCompleted = 0;

  const progress = $('decodeProgress');
  const filterB  = $('filterBar');
  const timeline = $('timeline');

  if (!txids.length) {
    if (progress) progress.style.display = 'none';
    if (timeline) {
      clear(timeline);
      timeline.appendChild(el('div', { class: 'empty fade' }, [
        el('div', { class: 'empty-icon', text: '∅' }),
        el('div', { class: 'empty-text', text: 'no transactions found for this address' }),
      ]));
    }
    return;
  }

  if (progress) {
    $('dpBar').style.width   = '0%';
    $('dpText').textContent  = 'decoding 0 / ' + txids.length + ' transactions…';
  }

  // Fire all decode requests in parallel via HTTP
  const decodePromises = txids.map((txid, i) =>
    fetch('/api/decode/' + encodeURIComponent(txid))
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(data => {
        decodeCompleted++;
        onDecodeArrival(data, i);
        updateProgress(decodeCompleted, txids.length);
      })
      .catch(() => {
        decodeCompleted++;
        updateProgress(decodeCompleted, txids.length);
      })
  );

  // When all done, finalize
  Promise.allSettled(decodePromises).then(() => {
    if (progress) progress.style.display = 'none';
    if (filterB)  {
      filterB.style.display = 'flex';
      updateFilterCount();
    }
    applyFilter(activeFilter);
  });
}

function updateProgress(done, total) {
  const bar  = $('dpBar');
  const txt  = $('dpText');
  const pct  = total > 0 ? Math.round(done / total * 100) : 0;
  if (bar) bar.style.width = pct + '%';
  if (txt) txt.textContent = 'decoding ' + done + ' / ' + total + ' transactions…';
}

// Called for each decoded tx as it arrives — renders row immediately
function onDecodeArrival(data, sequenceIndex) {
  const outputs      = Array.isArray(data.outputs) ? data.outputs : [];
  const unspentOuts  = outputs.filter(o => o.state === 'unspent');
  const spentOuts    = outputs.filter(o => o.state === 'spent');
  const totalUnspent = unspentOuts.reduce((s, o) => s + Number(o.amount || 0), 0);

  const decoded = {
    txid:         data.txid,
    outputs,
    totalUnspent,
    unspentCount: unspentOuts.length,
    spentCount:   spentOuts.length,
    seqIndex:     sequenceIndex,
  };

  currentDecoded.push(decoded);

  // Build and insert the row
  const row = buildTxRow(decoded);
  const timeline = $('timeline');
  if (!timeline) return;

  if (sortNewest) {
    // Insert at top (prepend) — later arrivals go above earlier ones
    timeline.prepend(row);
  } else {
    timeline.appendChild(row);
  }

  // Apply current filter to this new row immediately
  applyFilterToRow(row, activeFilter);
}

// =============================================================================
// TX row builder
// =============================================================================

function buildTxRow(decoded) {
  const { txid, outputs, totalUnspent, unspentCount, spentCount, seqIndex } = decoded;

  const head = el('div', { class: 'tx-row-head' }, [
    el('span', { class: 'tx-row-index', text: '#' + seqIndex }),
    el('span', {
      class: 'tx-row-txid',
      title: txid,
      text: txid,
      onClick: () => {
        setRaw({ txid, outputs }, 'decode · ' + txid.slice(0, 14) + '…');
        showView('raw');
      },
    }),
    el('span', {
      class: 'tx-row-meta',
      text: unspentCount + ' unspent · ' + spentCount + ' spent',
    }),
    el('span', {
      class: 'tx-row-total',
      text: totalUnspent > 0 ? '+ ' + toSnap(totalUnspent) + ' SNAP' : '',
    }),
  ]);

  const outputsEl = el('div', { class: 'tx-row-outputs' });

  for (const o of outputs) {
    const isUnspent = o.state === 'unspent';
    const outputRow = el('div', {
      class: 'tx-output',
      'data-state': o.state,
    }, [
      el('span', { class: 'tx-output-idx', text: String(o.index) }),
      el('span', { class: 'pill ' + o.state, text: o.state }),
      el('span', {
        class: 'tx-output-amt ' + (isUnspent ? 'is-unspent' : 'is-spent'),
        text: isUnspent
          ? '+ ' + (o.amount != null ? toSnap(o.amount) : '—')
          : '—',
      }),
    ]);

    // Address cell
    if (o.receiver) {
      outputRow.appendChild(el('span', {
        class: 'tx-output-addr' + (isUnspent ? '' : ' is-spent'),
        title: o.receiver,
        text: shortAddr(o.receiver, 32),
        onClick: isUnspent ? () => inspectWallet(o.receiver) : null,
      }));
    } else {
      outputRow.appendChild(el('span', {
        class: 'tx-output-addr is-spent',
        text: '—',
      }));
    }

    outputsEl.appendChild(outputRow);
  }

  const row = el('div', { class: 'tx-row' }, [head, outputsEl]);
  return row;
}

// =============================================================================
// Filter logic
// =============================================================================

function applyFilter(filter) {
  activeFilter = filter;

  // Update pill UI
  document.querySelectorAll('.fpill').forEach(p => {
    p.classList.toggle('on', p.dataset.filter === filter);
  });

  const timeline = $('timeline');
  if (!timeline) return;

  timeline.querySelectorAll('.tx-row').forEach(row => {
    applyFilterToRow(row, filter);
  });

  updateFilterCount();
}

function applyFilterToRow(row, filter) {
  if (filter === 'all') {
    row.querySelectorAll('.tx-output').forEach(o => o.classList.remove('hidden'));
    row.classList.remove('all-hidden');
    return;
  }

  let anyVisible = false;
  row.querySelectorAll('.tx-output').forEach(o => {
    const show = o.dataset.state === filter;
    o.classList.toggle('hidden', !show);
    if (show) anyVisible = true;
  });

  row.classList.toggle('all-hidden', !anyVisible);
}

function updateFilterCount() {
  const timeline = $('timeline');
  const count    = $('filterCount');
  if (!timeline || !count) return;

  const visible = timeline.querySelectorAll('.tx-row:not(.all-hidden)').length;
  count.textContent = visible + ' of ' + currentDecoded.length + ' transactions';
}

// =============================================================================
// Sort toggle — re-renders timeline from currentDecoded in new order
// =============================================================================

function toggleSort() {
  sortNewest = !sortNewest;
  const btn = $('btnSort');
  if (btn) btn.textContent = '↕ ' + (sortNewest ? 'Newest First' : 'Oldest First');

  const timeline = $('timeline');
  if (!timeline) return;
  clear(timeline);

  const ordered = sortNewest
    ? [...currentDecoded].reverse()
    : [...currentDecoded];

  for (const decoded of ordered) {
    const row = buildTxRow(decoded);
    timeline.appendChild(row);
    applyFilterToRow(row, activeFilter);
  }

  updateFilterCount();
}

// =============================================================================
// Wallet stats — HTTP, populates summary strip
// =============================================================================

async function fetchWalletStats(address) {
  try {
    const r = await fetch('/api/wallet_stats/' + encodeURIComponent(address));
    if (!r.ok) throw new Error(r.statusText);
    const d = await r.json();
    renderWalletSummary(d);
  } catch (e) {
    // Non-fatal — summary strip just stays hidden
  }
}

function renderWalletSummary(d) {
  const summary = $('walletSummary');
  if (!summary) return;

  // Address
  const addrEl = $('wsSummaryAddr');
  if (addrEl) {
    addrEl.textContent = shortAddr(d.address, 40);
    addrEl.title       = d.address;
    addrEl.onclick     = () => inspectWallet(d.address);
  }

  // Trend badge
  const trendEl = $('wsTrend');
  if (trendEl) {
    const arrows = { rising: '↑ rising', falling: '↓ falling', stable: '→ stable' };
    trendEl.textContent = arrows[d.trend] || d.trend;
    trendEl.className   = 'ws-trend ' + (d.trend || 'stable');
  }

  // Stats
  const set = (id, v) => { const e = $(id); if (e) e.textContent = v; };
  set('wsSummaryBalance',   toSnap(d.total_unspent_atomic) + ' SNAP');
  set('wsSummaryTxCount',   numFmt(d.tx_count));
  set('wsSummaryUnspent',   numFmt(d.unspent_count));
  set('wsSummarySpent',     numFmt(d.spent_count));
  set('wsSummaryLargest',   toSnap(d.largest_output_atomic) + ' SNAP');
  set('wsSummaryAvg',       toSnap(d.avg_output_atomic) + ' SNAP');
  set('wsSummarySpendRate', d.spend_rate_pct.toFixed(1) + '%');
  set('wsSummaryFrag',      d.fragmentation);
  set('wsSummaryDelta',     d.delta_snap + ' SNAP');

  // Delta color
  const deltaEl = $('wsSummaryDelta');
  if (deltaEl) {
    deltaEl.style.color = d.delta_atomic > 0
      ? 'var(--green)'
      : d.delta_atomic < 0
        ? 'var(--red)'
        : 'var(--muted)';
  }

  // Fragmentation color
  const fragEl = $('wsSummaryFrag');
  if (fragEl) {
    fragEl.style.color = d.fragmentation === 'HIGH'
      ? 'var(--red)'
      : d.fragmentation === 'MEDIUM'
        ? 'var(--amber)'
        : 'var(--green)';
  }

  // First / last seen txids
  const first = $('wsFirstSeen');
  if (first) {
    first.textContent = shortAddr(d.first_seen_txid, 20);
    first.title       = d.first_seen_txid;
    first.onclick     = () => {
      setRaw(d.first_seen_txid, 'first seen txid');
      showView('raw');
    };
  }

  const last = $('wsLastSeen');
  if (last) {
    last.textContent = shortAddr(d.last_seen_txid, 20);
    last.title       = d.last_seen_txid;
    last.onclick     = () => {
      setRaw(d.last_seen_txid, 'last seen txid');
      showView('raw');
    };
  }

  summary.style.display = 'block';
}

// =============================================================================
// Top receivers renderer — renders into view-top
// =============================================================================

const COLORS = [
  '#f0a832','#e8425a','#2ec97a','#4d9ef5','#b87ef0',
  '#f07832','#5ac9e0','#c9e032','#e032c9','#32e0a8',
  '#f0c832','#e87842','#428ae8','#42e874',
];

function renderTopReceivers(data) {
  const listCard = $('recvListCard');
  const pieCard  = $('recvPieCard');
  const meta     = $('recvMeta');
  const pieMeta  = $('recvPieMeta');
  const pieWrap  = $('pieWrap');

  if (!listCard) return;

  // Remove existing rows (keep card-head)
  Array.from(listCard.children).forEach(c => {
    if (!c.classList.contains('card-head')) listCard.removeChild(c);
  });

  if (!data.length) {
    listCard.appendChild(el('div', { class: 'empty' },
      el('div', { class: 'empty-text', text: 'no data' })
    ));
    showView('top');
    return;
  }

  const total = data.reduce((s, r) => s + Number(r.total_unspent), 0);
  const max   = Number(data[0].total_unspent) || 1;

  if (meta)    meta.textContent    = 'total ' + toSnap(total) + ' SNAP';
  if (pieMeta) pieMeta.textContent = data.length + ' receivers shown';

  for (let i = 0; i < data.length; i++) {
    const r   = data[i];
    const pct = (Number(r.total_unspent) / max * 100).toFixed(1);
    const col = COLORS[i % COLORS.length];

    const row = el('div', { class: 'recv-row' }, [
      el('span', { class: 'recv-rank', text: String(i + 1) }),
      el('div',  { class: 'recv-bar-wrap' },
        el('div', { class: 'recv-bar', style: `width:${pct}%;background:${col}` })
      ),
      el('span', { class: 'recv-val', text: toSnap(r.total_unspent) }),
      el('span', {
        class: 'recv-addr-col',
        title: r.receiver_base36,
        onClick: () => inspectWallet(r.receiver_base36),
        text: shortAddr(r.receiver_base36, 14),
      }),
    ]);
    listCard.appendChild(row);
  }

  // Pie chart
  if (pieWrap) {
    clear(pieWrap);
    pieWrap.appendChild(buildPieSvg(data, total));

    const legend = el('div', { class: 'pie-legend' });
    for (let i = 0; i < Math.min(14, data.length); i++) {
      const r   = data[i];
      const pct = (Number(r.total_unspent) / total * 100).toFixed(1);
      const col = COLORS[i % COLORS.length];

      legend.appendChild(el('div', { class: 'pie-leg-row' }, [
        el('div',  { class: 'pie-dot', style: `background:${col}` }),
        el('span', {
          class: 'pie-leg-addr',
          title: r.receiver_base36,
          onClick: () => inspectWallet(r.receiver_base36),
          text: shortAddr(r.receiver_base36, 16),
        }),
        el('span', { class: 'pie-leg-val', text: toSnap(r.total_unspent) }),
        el('span', { class: 'pie-leg-pct', text: pct + '%' }),
      ]));
    }
    pieWrap.appendChild(legend);
  }

  showView('top');
}

// =============================================================================
// Pie SVG builder
// =============================================================================

function buildPieSvg(data, total) {
  const S = 180, cx = S / 2, cy = S / 2, r = S / 2 - 6, ri = r * 0.42;
  let angle = -Math.PI / 2;

  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('width',   String(S));
  svg.setAttribute('height',  String(S));
  svg.setAttribute('viewBox', `0 0 ${S} ${S}`);

  data.forEach((item, i) => {
    const frac  = Number(item.total_unspent) / total;
    const sweep = frac * Math.PI * 2;
    const x1    = cx + r * Math.cos(angle);
    const y1    = cy + r * Math.sin(angle);
    angle += sweep;
    const x2    = cx + r * Math.cos(angle);
    const y2    = cy + r * Math.sin(angle);
    const large = sweep > Math.PI ? 1 : 0;
    const col   = COLORS[i % COLORS.length];

    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    path.setAttribute('d',
      `M${cx},${cy} L${x1.toFixed(2)},${y1.toFixed(2)} A${r},${r} 0 ${large},1 ${x2.toFixed(2)},${y2.toFixed(2)} Z`
    );
    path.setAttribute('fill',         col);
    path.setAttribute('opacity',      '.88');
    path.setAttribute('stroke',       'var(--bg)');
    path.setAttribute('stroke-width', '1.5');
    svg.appendChild(path);
  });

  const hole = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
  hole.setAttribute('cx',   String(cx));
  hole.setAttribute('cy',   String(cy));
  hole.setAttribute('r',    ri.toFixed(1));
  hole.setAttribute('fill', 'var(--surface)');
  svg.appendChild(hole);

  const makeText = (y, text, size, weight, fill) => {
    const t = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    t.setAttribute('x',           String(cx));
    t.setAttribute('y',           String(y));
    t.setAttribute('text-anchor', 'middle');
    t.setAttribute('fill',        fill);
    t.setAttribute('font-family', 'IBM Plex Mono,monospace');
    t.setAttribute('font-size',   String(size));
    t.setAttribute('font-weight', String(weight));
    t.textContent = text;
    return t;
  };

  svg.appendChild(makeText(cy + 4,  String(data.length), 13, 600, 'var(--amber)'));
  svg.appendChild(makeText(cy + 16, 'receivers',          8, 400, 'var(--muted)'));
  return svg;
}

// =============================================================================
// Button wiring
// =============================================================================

const btnInspect = $('btnInspect');
if (btnInspect) btnInspect.onclick = () => {
  const a = $('addr')?.value?.trim();
  if (a) inspectWallet(a);
};

const btnClear = $('btnClear');
if (btnClear) btnClear.onclick = () => {
  const inp = $('addr');
  if (inp) inp.value = '';
  currentAddress = '';
  showView('empty');
};

const btnTop = $('btnTop');
if (btnTop) btnTop.onclick = () => {
  wsSend({ cmd: 'top_receivers', limit: 9999 });
};

const btnRawJson = $('btnRawJson');
if (btnRawJson) btnRawJson.onclick = () => showView('raw');

const btnSort = $('btnSort');
if (btnSort) btnSort.onclick = () => toggleSort();

const wsPill = $('wsPill');
if (wsPill) wsPill.onclick = () => connect();

// Filter pills
document.querySelectorAll('.fpill').forEach(pill => {
  pill.addEventListener('click', () => applyFilter(pill.dataset.filter));
});

// Enter key in address input triggers inspect
const addrInp = $('addr');
if (addrInp) addrInp.addEventListener('keydown', e => {
  if (e.key === 'Enter') {
    const a = addrInp.value.trim();
    if (a) inspectWallet(a);
  }
});

// Ctrl/Cmd+Enter also fires inspect
document.addEventListener('keydown', e => {
  if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
    const a = $('addr')?.value?.trim();
    if (a) inspectWallet(a);
  }
});

// =============================================================================
// Boot
// =============================================================================

connect();

})();

// =============================================================================
// static/db_dashboard.js
// snapcoin-db-inspector/static/db_dashboard.js
// Created: 2026-02-26T00:00:00Z
// =============================================================================
