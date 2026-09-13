/*
 * DistribuKV dashboard UI. One UI, two backends:
 *   KvDashboard.liveAdapter(baseUrl)   the real cluster's HTTP API
 *   KvDashboard.simulationAdapter()    KvSim (docs/sim.js), running in the browser
 * Requires sim.js (ring placement is computed with the same hash as the Java code).
 */
(function (root) {
  'use strict';

  const NODE_COLORS = 8;
  const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const clock = () => new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  const list = items => items.length <= 1 ? items.join('') : items.slice(0, -1).join(', ') + ' and ' + items[items.length - 1];
  const KEY_PATTERN = /^[A-Za-z0-9._:-]{1,256}$/;

  // ---- Adapters ----------------------------------------------------------------------------------

  function liveAdapter(base = '') {
    const readBody = async res => {
      const text = await res.text();
      try { return JSON.parse(text); } catch (e) { return { message: text.slice(0, 300) }; }
    };
    return {
      mode: 'live',
      canCrash: false,
      canChooseCoordinator: false,
      async status() {
        const res = await fetch(base + '/cluster/status', { cache: 'no-store' });
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const s = await res.json();
        return {
          self: s.self,
          replicationFactor: s.replicationFactor,
          virtualNodes: s.virtualNodes || 256,
          stack: s.stack || {},
          nodes: s.nodes.map(n => ({
            id: n.id,
            state: !n.stats ? 'DOWN' : n.stats.isolated ? 'ISOLATED' : n.status,
            keys: n.stats ? n.stats.keys : null,
            hints: n.stats ? n.stats.hintsPending : null,
            lagMs: n.stats && typeof n.stats.replicationLagMs === 'number' && n.stats.replicationLagMs >= 0 ? n.stats.replicationLagMs : null,
            heartbeatMs: n.id === s.self ? 0 : n.lastSeenMsAgo
          }))
        };
      },
      async send(op, key, value, consistency) {
        const started = performance.now();
        const res = await fetch(`${base}/kv/${encodeURIComponent(key)}?consistency=${consistency}`, {
          method: op,
          headers: { 'Content-Type': 'text/plain' },
          body: op === 'PUT' ? value : undefined
        });
        const body = await readBody(res);
        return { status: res.status, body, ms: Math.round(performance.now() - started), events: [] };
      },
      async setIsolated(id, enabled) {
        const res = await fetch(`${base}/cluster/nodes/${encodeURIComponent(id)}/isolate?enabled=${enabled}`, { method: 'POST' });
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return [];
      }
    };
  }

  function simulationAdapter() {
    let cluster = new root.KvSim.Cluster();
    const pickCoordinator = key => {
      const replicas = cluster.replicasFor(key);
      return cluster.ids.find(id => cluster.isUp(id) && !replicas.includes(id))
        || cluster.ids.find(id => cluster.isUp(id)) || cluster.ids[0];
    };
    return {
      mode: 'simulation',
      canCrash: true,
      canChooseCoordinator: true,
      async status() {
        return {
          self: null,
          replicationFactor: cluster.n,
          virtualNodes: 256,
          stack: { storage: 'mysql', replicationLog: 'kafka:kv-replication', membership: 'redis' },
          nodes: cluster.ids.map(id => {
            const node = cluster.nodes.get(id);
            const up = node.state === 'UP';
            return { id, state: node.state === 'CRASHED' ? 'DOWN' : node.state, keys: cluster.liveKeys(id), hints: node.hints.length, lagMs: up ? 0 : null, heartbeatMs: up ? 0 : null };
          })
        };
      },
      async send(op, key, value, consistency, via) {
        const coordinator = via && via !== 'auto' ? via : pickCoordinator(key);
        const result = op === 'PUT' ? cluster.put(coordinator, key, value, consistency)
          : op === 'GET' ? cluster.get(coordinator, key, consistency)
          : cluster.delete(coordinator, key, consistency);
        return { status: result.status, body: result.body, ms: null, events: result.events, coordinator };
      },
      async setIsolated(id, enabled) { return cluster.setState(id, enabled ? 'ISOLATED' : 'UP'); },
      async setCrashed(id, crashed) { return cluster.setState(id, crashed ? 'CRASHED' : 'UP'); },
      async reset() { cluster = new root.KvSim.Cluster(); return ['Cluster reset: 5 healthy nodes, N = 3.']; }
    };
  }

  // ---- Ring geometry -----------------------------------------------------------------------------

  const angleOf = token => Number(BigInt.asUintN(64, token) >> 11n) / 9007199254740992 * 360;
  const polar = (r, deg) => { const a = (deg - 90) * Math.PI / 180; return [150 + r * Math.cos(a), 150 + r * Math.sin(a)]; };
  const arc = (r, from, to) => {
    const [x0, y0] = polar(r, from);
    const [x1, y1] = polar(r, to);
    return `M${x0.toFixed(2)} ${y0.toFixed(2)}A${r} ${r} 0 ${to - from > 180 ? 1 : 0} 1 ${x1.toFixed(2)} ${y1.toFixed(2)}`;
  };

  function ringModel(ids, virtualNodes, replicationFactor) {
    const ring = new root.KvSim.Ring(ids, virtualNodes);
    const points = ring.tokens.map(([token, id]) => ({ angle: angleOf(token), id })).sort((a, b) => a.angle - b.angle);
    const paths = Object.fromEntries(ids.map(id => [id, '']));
    const share = Object.fromEntries(ids.map(id => [id, 0]));
    for (let i = 0; i < points.length; i++) {
      const prev = points[(i - 1 + points.length) % points.length].angle;
      const { angle, id } = points[i];
      const pieces = angle >= prev ? [[prev, angle]] : [[prev, 360], [0, angle]];
      for (const [from, to] of pieces) {
        if (to - from < 0.01) continue;
        paths[id] += arc(118, from, to);
        share[id] += (to - from) / 360;
      }
    }
    return {
      paths, share,
      replicasFor: key => ring.preferenceList(key, replicationFactor),
      keyAngle: key => angleOf(root.KvSim.hash(key))
    };
  }

  // ---- UI ----------------------------------------------------------------------------------------

  function mount(el, adapter, options = {}) {
    const live = adapter.mode === 'live';
    const s = {
      key: options.key || 'user:42', value: options.value || 'alice', consistency: 'STRONG', via: 'auto',
      status: null, reachable: true, response: null, feed: [], busy: false, ring: null, ringKey: ''
    };
    const colorOf = id => {
      const index = s.status ? s.status.nodes.findIndex(n => n.id === id) : 0;
      return `var(--kvd-n${(index % NODE_COLORS) + 1})`;
    };

    el.classList.add('kvd');
    el.innerHTML = `
      <div class="kvd-top">
        <div class="kvd-brand">
          <span class="kvd-logo" aria-hidden="true"></span>
          <span class="kvd-title">DistribuKV</span>
          <span class="kvd-mode" data-r="mode"></span>
        </div>
        <div class="kvd-links" data-r="links"></div>
      </div>
      <div data-r="banner"></div>
      <div class="kvd-kpis" data-r="kpis"></div>
      <div class="kvd-main">
        <div class="kvd-card kvd-ring">
          <h3 class="kvd-h"><span>Consistent hash ring</span><span data-r="ringmeta"></span></h3>
          <div data-r="ring"></div>
          <ul class="kvd-legend" data-r="legend"></ul>
          <p class="kvd-ring-caption" data-r="caption"></p>
        </div>
        <div class="kvd-card kvd-console">
          <h3 class="kvd-h"><span>Send a request</span><span data-r="coordinfo"></span></h3>
          <div class="kvd-form">
            <div class="kvd-field"><label for="kvd-key-${el.id}">Key</label><input class="kvd-input" id="kvd-key-${el.id}" data-r="key" autocomplete="off" spellcheck="false"></div>
            <div class="kvd-field"><label for="kvd-val-${el.id}">Value</label><input class="kvd-input" id="kvd-val-${el.id}" data-r="value" autocomplete="off" spellcheck="false"></div>
          </div>
          <div class="kvd-row">
            <div class="kvd-seg" role="group" aria-label="Consistency">
              <button type="button" data-consistency="STRONG" aria-pressed="true">STRONG</button>
              <button type="button" data-consistency="EVENTUAL" aria-pressed="false">EVENTUAL</button>
            </div>
            <span class="kvd-seg-hint" data-r="seghint"></span>
            ${adapter.canChooseCoordinator ? '<select class="kvd-select" data-r="via" aria-label="Coordinator" style="width:auto"></select>' : ''}
            <div class="kvd-actions">
              <button class="kvd-btn primary" data-op="PUT">PUT</button>
              <button class="kvd-btn" data-op="GET">GET</button>
              <button class="kvd-btn" data-op="DELETE">DELETE</button>
            </div>
          </div>
          <div class="kvd-resp" data-r="resp" aria-live="polite"></div>
        </div>
      </div>
      <div class="kvd-nodes" data-r="nodes"></div>
      <div class="kvd-bottom">
        <div class="kvd-card kvd-scen">
          <h3 class="kvd-h"><span>Break it on purpose</span>${adapter.reset ? '<button class="kvd-btn sm" data-scenario="reset">Reset</button>' : ''}</h3>
          <ol class="kvd-steps">
            <li class="kvd-step"><span class="n">1</span><div><b>${live ? 'Partition' : 'Kill'} one replica</b><span>STRONG still has a 2 of 3 quorum</span></div><button class="kvd-btn sm" data-scenario="one">Run</button></li>
            <li class="kvd-step"><span class="n">2</span><div><b>${live ? 'Partition' : 'Kill'} a second replica</b><span>STRONG fails with 503, EVENTUAL still works</span></div><button class="kvd-btn sm" data-scenario="two">Run</button></li>
            <li class="kvd-step"><span class="n">3</span><div><b>Bring everything back</b><span>Hints and the Kafka log repair missed writes</span></div><button class="kvd-btn sm" data-scenario="heal">Run</button></li>
          </ol>
        </div>
        <div class="kvd-card kvd-activity">
          <h3 class="kvd-h"><span>Activity</span></h3>
          <ul class="kvd-feed" data-r="feed"></ul>
        </div>
      </div>
      <p class="kvd-foot" data-r="foot"></p>`;

    const r = name => el.querySelector(`[data-r="${name}"]`);
    r('key').value = s.key;
    r('value').value = s.value;

    function log(text, tone = 'info') {
      s.feed.unshift({ time: clock(), text, tone });
      s.feed.length = Math.min(s.feed.length, 60);
    }

    // -- rendering

    function render() {
      renderTop();
      renderKpis();
      renderRing();
      renderNodes();
      renderResponse();
      renderFeed();
    }

    function renderTop() {
      const mode = r('mode');
      if (live) {
        mode.className = 'kvd-mode ' + (s.reachable ? 'live' : 'offline');
        mode.innerHTML = `<span class="kvd-pulse"></span>${s.reachable ? `Live cluster${s.status ? ' · served by ' + esc(s.status.self) : ''}` : 'Cluster unreachable'}`;
      } else {
        mode.className = 'kvd-mode sim';
        mode.innerHTML = '<span class="kvd-pulse"></span>Simulation · runs in your browser';
      }
      const stack = s.status ? s.status.stack : {};
      const chips = s.status ? `
        <span class="kvd-chip">storage <b>${esc(stack.storage || '–')}</b></span>
        <span class="kvd-chip">replication log <b>${esc((stack.replicationLog || 'none').replace('kafka:', 'kafka · '))}</b></span>
        <span class="kvd-chip">failure detection <b>${esc(stack.membership || '–')}</b></span>` : '';
      const links = (options.links || []).map(l => `<a class="kvd-link" href="${esc(l.href)}" target="_blank" rel="noopener">${esc(l.label)}</a>`).join('');
      r('links').innerHTML = chips + links;
      r('banner').innerHTML = live && !s.reachable
        ? '<div class="kvd-banner">Can\'t reach the cluster right now. Retrying every few seconds…</div>' : '';
      r('foot').textContent = live
        ? 'Real cluster: every request above hits the Java nodes. Partitions heal automatically on the public demo.'
        : 'Simulation of the same algorithms as the Java code (ring placement, quorums, last-write-wins, hints, read repair, Kafka log). No server involved.';
    }

    function renderKpis() {
      const nodes = s.status ? s.status.nodes : [];
      const n = s.status ? s.status.replicationFactor : 3;
      const up = nodes.filter(x => x.state === 'UP').length;
      const copies = nodes.reduce((sum, x) => sum + (x.keys > 0 ? x.keys : 0), 0);
      const hints = nodes.reduce((sum, x) => sum + (x.hints > 0 ? x.hints : 0), 0);
      const lags = nodes.map(x => x.lagMs).filter(x => typeof x === 'number');
      const lag = lags.length ? Math.max(...lags) : null;
      const quorum = Math.floor(n / 2) + 1;
      const tile = (label, value, note, tone = '') => `<div class="kvd-card kvd-kpi ${tone}"><span>${label}</span><strong>${value}</strong><small>${note}</small></div>`;
      r('kpis').innerHTML = [
        tile('Healthy nodes', s.status ? `${up}/${nodes.length}` : '–', up === nodes.length ? 'all nodes serving' : `${nodes.length - up} unavailable`, !s.status ? '' : up === nodes.length ? 'good' : up >= quorum ? 'warn' : 'bad'),
        tile('Replication', `N = ${n}`, `STRONG needs ${quorum} of ${n}`),
        tile('Stored copies', s.status ? copies.toLocaleString() : '–', 'keys × replicas'),
        tile('Hints queued', s.status ? hints : '–', hints ? 'waiting for replicas to return' : 'no missed writes pending', hints ? 'warn' : ''),
        tile('Kafka log lag', lag === null ? '–' : lag < 1000 ? `${lag} ms` : `${(lag / 1000).toFixed(1)} s`, 'latest consumed write')
      ].join('');
    }

    function currentReplicas() {
      return s.ring && KEY_PATTERN.test(s.key) ? s.ring.replicasFor(s.key) : [];
    }

    function renderRing() {
      if (!s.status) { r('ring').innerHTML = ''; return; }
      const replicas = currentReplicas();
      const states = Object.fromEntries(s.status.nodes.map(x => [x.id, x.state]));
      const validKey = KEY_PATTERN.test(s.key);
      const angle = validKey ? s.ring.keyAngle(s.key) : null;
      const segments = s.status.nodes.map(node => {
        const cls = states[node.id] !== 'UP' ? 'seg down' : replicas.length && !replicas.includes(node.id) ? 'seg dim' : 'seg';
        return `<path class="${cls}" style="stroke:${colorOf(node.id)}" d="${s.ring.paths[node.id]}"/>`;
      }).join('');
      let marker = '';
      if (angle !== null) {
        const [x0, y0] = polar(90, angle);
        const [x1, y1] = polar(146, angle);
        marker = `<line class="keyline" x1="${x0.toFixed(1)}" y1="${y0.toFixed(1)}" x2="${x1.toFixed(1)}" y2="${y1.toFixed(1)}"/><circle class="keydot" cx="${x1.toFixed(1)}" cy="${y1.toFixed(1)}" r="4.5"/>`;
      }
      r('ring').innerHTML = `<svg viewBox="0 0 300 300" role="img" aria-label="Hash ring: ${s.status.nodes.length} nodes with ${s.status.virtualNodes} virtual nodes each; key ${esc(s.key)} maps to ${esc(replicas.join(', '))}">
        <circle class="track" cx="150" cy="150" r="118"/>${segments}${marker}
        <text class="center-big" x="150" y="146" text-anchor="middle">${replicas.length ? 'N = ' + replicas.length : '–'}</text>
        <text class="center-small" x="150" y="166" text-anchor="middle">${s.status.nodes.length} nodes · ${s.status.virtualNodes} vnodes each</text>
      </svg>`;
      r('ringmeta').textContent = validKey ? `key at ${angle.toFixed(1)}°` : '';
      r('legend').innerHTML = s.status.nodes.map(node => {
        const rank = replicas.indexOf(node.id);
        return `<li><span class="sw" style="--c:${colorOf(node.id)}"></span>${esc(node.id)}${rank >= 0 ? ` <span class="rk">replica ${rank + 1}</span>` : ''}<span class="own">${(s.ring.share[node.id] * 100).toFixed(1)}% of ring</span></li>`;
      }).join('');
      r('caption').innerHTML = validKey
        ? `<b>${esc(s.key)}</b> is hashed onto the ring; walking clockwise, the first ${replicas.length} distinct nodes store it: ${esc(list(replicas))}.`
        : 'Keys may use letters, digits and <code>. _ : -</code>';
      const via = r('via');
      if (via) {
        const chosen = s.via;
        via.innerHTML = '<option value="auto">Coordinator: any healthy node</option>'
          + s.status.nodes.map(x => `<option value="${esc(x.id)}">Coordinator: ${esc(x.id)}${x.state !== 'UP' ? ' (' + x.state.toLowerCase() + ')' : ''}</option>`).join('');
        via.value = chosen;
      }
      r('coordinfo').textContent = live && s.status ? `coordinator: ${s.status.self}` : '';
      r('seghint').textContent = s.consistency === 'STRONG' ? `waits for ${Math.floor(replicas.length / 2) + 1} of ${replicas.length || 3} replicas` : 'acknowledged once durable';
      el.querySelectorAll('[data-consistency]').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.consistency === s.consistency)));
    }

    function renderNodes() {
      if (!s.status) { r('nodes').innerHTML = ''; return; }
      const replicas = currentReplicas();
      r('nodes').innerHTML = s.status.nodes.map(node => {
        const rank = replicas.indexOf(node.id);
        const down = node.state !== 'UP';
        const role = rank >= 0 ? `Replica ${rank + 1} of ${replicas.length} for ${s.key}` : node.id === s.status.self ? 'Serving this dashboard' : 'Not a replica of this key';
        const fmt = (v, suffix = '') => (v === null || v === undefined || v < 0) ? '–' : v + suffix;
        const isolateLabel = node.state === 'ISOLATED' ? 'Heal' : 'Isolate';
        return `<div class="kvd-card kvd-node ${rank >= 0 ? 'replica' : ''} ${down ? 'is-down' : ''}" style="--c:${colorOf(node.id)}">
          <div class="kvd-node-head"><strong>${esc(node.id)}</strong><span class="kvd-badge ${node.state}">${node.state}</span></div>
          <div class="kvd-role">${esc(role)}</div>
          <dl>
            <dt>Keys stored</dt><dd>${fmt(node.keys)}</dd>
            <dt>Hints queued</dt><dd>${fmt(node.hints)}</dd>
            <dt>Log lag</dt><dd>${fmt(node.lagMs, ' ms')}</dd>
            <dt>Heartbeat</dt><dd>${node.state === 'UP' ? (node.heartbeatMs ? node.heartbeatMs + ' ms ago' : 'ok') : '–'}</dd>
          </dl>
          <div class="kvd-node-actions">
            ${adapter.canCrash ? `<button class="kvd-btn sm danger" data-crash="${esc(node.id)}">${node.state === 'DOWN' ? 'Restart' : 'Kill'}</button>` : ''}
            <button class="kvd-btn sm" data-isolate="${esc(node.id)}" ${node.state === 'DOWN' ? 'disabled' : ''}>${isolateLabel}</button>
          </div>
        </div>`;
      }).join('');
    }

    function renderResponse() {
      const box = r('resp');
      const res = s.response;
      if (!res) {
        box.innerHTML = '<p class="kvd-placeholder">Press PUT to store the key on its 3 replicas, then try the scenarios below and send it again.</p>';
        return;
      }
      const b = res.body || {};
      const tone = res.status >= 200 && res.status < 300 ? 'ok' : res.status === 404 ? 'miss' : 'err';
      const httpLabel = res.status === 0 ? 'NO CONNECTION' : 'HTTP ' + res.status;
      const replicas = b.replicas || [];
      const confirmed = b.acknowledgedBy || b.respondedBy || [];
      let summary;
      if (res.status === 0) summary = esc(b.message || 'Request failed');
      else if (b.error === 'quorum_not_reached') summary = `Quorum not reached: needed <b>${b.required}</b> replicas, only ${confirmed.length ? esc(list(confirmed)) : 'none'} answered. ${res.op === 'PUT' || res.op === 'DELETE' ? 'Try EVENTUAL.' : ''}`;
      else if (res.status >= 400 && res.status !== 404) summary = esc(b.message || b.error || 'Request failed');
      else if (res.op === 'GET' && b.found) summary = `Value <b>“${esc(b.value)}”</b>, newest version from ${esc(list(confirmed))}.`;
      else if (res.op === 'GET') summary = 'Not found (the key was never written, or it was deleted).';
      else if (b.replicationLog) summary = `Durable in the Kafka log at <b>${esc(b.replicationLog.replace('kafka:', ''))}</b>; ${confirmed.length ? esc(list(confirmed)) + ' applied it right away' : 'replicas will apply it from the log'}.`;
      else summary = `${res.op === 'DELETE' ? 'Deleted (tombstone written)' : 'Stored'} on ${esc(list(confirmed))}: quorum ${confirmed.length} of ${replicas.length} (needed ${b.requiredAcks}).`;
      const tags = replicas.length ? `<div class="kvd-tags">${replicas.map(id => `<span class="kvd-tag ${confirmed.includes(id) ? '' : 'no'}" style="--c:${colorOf(id)}"><i></i>${esc(id)}</span>`).join('')}</div>` : '';
      box.innerHTML = `
        <div class="kvd-resp-head"><span class="kvd-http ${tone}">${httpLabel}</span>
          <span class="kvd-resp-meta">${esc(res.op)} ${esc(res.key)} · ${esc(res.consistency)}${res.coordinator ? ' · via ' + esc(res.coordinator) : ''}${typeof res.ms === 'number' ? ' · ' + res.ms + ' ms' : ''}</span></div>
        <p class="kvd-summary">${summary}</p>${tags}
        <details><summary>Raw JSON response</summary><pre class="kvd-pre">${esc(JSON.stringify(b, null, 2))}</pre></details>`;
    }

    function renderFeed() {
      r('feed').innerHTML = s.feed.length
        ? s.feed.map(item => `<li class="${item.tone}"><time>${item.time}</time><span class="mark"></span><span>${esc(item.text)}</span></li>`).join('')
        : `<li class="info"><time>${clock()}</time><span class="mark"></span><span>${live ? 'Connected. Watching the cluster.' : '5 simulated nodes are up. Send a request to begin.'}</span></li>`;
    }

    // -- data

    function diffStatus(before, after) {
      if (!live || !before) return;
      const old = Object.fromEntries(before.nodes.map(x => [x.id, x]));
      for (const node of after.nodes) {
        const prev = old[node.id];
        if (!prev) continue;
        if (prev.state !== node.state) {
          log(node.state === 'UP' ? `${node.id} is back UP` : `${node.id} is ${node.state}`, node.state === 'UP' ? 'ok' : 'err');
        }
        if (prev.hints > 0 && node.hints === 0) log(`${node.id} delivered its queued hints (hinted handoff)`, 'ok');
        if ((prev.hints || 0) === 0 && node.hints > 0) log(`${node.id} is holding ${node.hints} hint(s) for unavailable replicas`, 'warn');
      }
    }

    async function refresh() {
      try {
        const status = await adapter.status();
        diffStatus(s.status, status);
        s.status = status;
        if (!s.reachable) log('Connection to the cluster restored', 'ok');
        s.reachable = true;
        const ids = status.nodes.map(x => x.id).join(',');
        if (!s.ring || s.ringKey !== ids) {
          s.ring = ringModel(status.nodes.map(x => x.id), status.virtualNodes, status.replicationFactor);
          s.ringKey = ids;
        }
      } catch (e) {
        if (s.reachable) log('Lost connection to the cluster', 'err');
        s.reachable = false;
      }
      render();
    }

    async function send(op) {
      if (s.busy) return;
      if (!KEY_PATTERN.test(s.key)) {
        s.response = { op, key: s.key, consistency: s.consistency, status: 400, body: { error: 'bad_request', message: 'Keys must match [A-Za-z0-9._:-]{1,256}' } };
        renderResponse();
        return;
      }
      s.busy = true;
      try {
        const result = await adapter.send(op, s.key, s.value, s.consistency, s.via);
        s.response = { op, key: s.key, consistency: s.consistency, ...result };
        const b = result.body || {};
        const ok = result.status >= 200 && result.status < 300;
        log(`${op} ${s.key} (${s.consistency})${result.coordinator ? ' via ' + result.coordinator : ''} → ${result.status === 0 ? 'no connection' : 'HTTP ' + result.status}${typeof result.ms === 'number' ? ' in ' + result.ms + ' ms' : ''}`, ok ? 'ok' : result.status === 404 ? 'warn' : 'err');
        for (const text of result.events || []) log(text, /hint|unreachable|rolled back/.test(text) ? 'warn' : 'info');
        if (b.error === 'quorum_not_reached') log('Not enough replicas answered for the STRONG quorum', 'err');
      } catch (e) {
        s.response = { op, key: s.key, consistency: s.consistency, status: 0, body: { message: 'Could not reach the cluster: ' + e.message } };
        log(`${op} failed: could not reach the cluster`, 'err');
      } finally {
        s.busy = false;
      }
      await refresh();
    }

    async function act(fn, description, tone) {
      try {
        const events = await fn();
        if (description) log(description, tone);
        for (const text of events || []) log(text, /crashed|partitioned|lost/.test(text) ? 'err' : /UP|healed|delivered|caught up/.test(text) ? 'ok' : 'info');
      } catch (e) {
        log('Action failed: ' + e.message, 'err');
      }
      await refresh();
    }

    const stateOf = id => s.status.nodes.find(x => x.id === id).state;
    const breakNode = id => adapter.canCrash ? adapter.setCrashed(id, true) : adapter.setIsolated(id, true);

    async function scenario(name) {
      if (!s.status) return;
      if (name === 'reset') {
        s.response = null;
        s.feed = [];
        return act(() => adapter.reset(), null);
      }
      if (!KEY_PATTERN.test(s.key)) return;
      // Never partition the node serving the live dashboard: requests go through it.
      const candidates = currentReplicas().filter(id => !live || id !== s.status.self);
      const verb = adapter.canCrash ? 'Killed' : 'Partitioned';
      if (name === 'one') {
        const target = candidates.find(id => stateOf(id) === 'UP');
        if (!target) return;
        await act(() => breakNode(target), `Scenario 1: ${verb.toLowerCase()} ${target}, a replica of ${s.key}. STRONG still has 2 of 3: send PUT or GET.`, 'warn');
      } else if (name === 'two') {
        const targets = candidates.slice(0, 2);
        await act(async () => {
          const events = [];
          for (const id of targets) if (stateOf(id) === 'UP') events.push(...await breakNode(id));
          return events;
        }, `Scenario 2: ${targets.join(' and ')} are unavailable. A STRONG PUT now fails with 503; switch to EVENTUAL and it succeeds.`, 'warn');
      } else if (name === 'heal') {
        await act(async () => {
          const events = [];
          for (const node of s.status.nodes) {
            if (node.state === 'ISOLATED') events.push(...await adapter.setIsolated(node.id, false));
            else if (node.state === 'DOWN' && adapter.canCrash) events.push(...await adapter.setCrashed(node.id, false));
          }
          return events;
        }, 'Scenario 3: bringing every node back. Missed writes arrive via hints and the Kafka log.', 'ok');
      }
    }

    // -- events

    el.addEventListener('click', event => {
      const button = event.target.closest('button');
      if (!button || !el.contains(button)) return;
      const d = button.dataset;
      if (d.op) send(d.op);
      else if (d.consistency) { s.consistency = d.consistency; renderRing(); }
      else if (d.scenario) scenario(d.scenario);
      else if (d.crash) act(() => adapter.setCrashed(d.crash, stateOf(d.crash) !== 'DOWN'));
      else if (d.isolate) act(() => adapter.setIsolated(d.isolate, stateOf(d.isolate) !== 'ISOLATED'));
    });
    r('key').addEventListener('input', e => { s.key = e.target.value.trim(); renderRing(); renderNodes(); });
    r('value').addEventListener('input', e => { s.value = e.target.value; });
    el.addEventListener('change', e => { if (e.target.dataset.r === 'via') s.via = e.target.value; });
    el.addEventListener('keydown', e => { if (e.key === 'Enter' && e.target.classList.contains('kvd-input')) send('PUT'); });

    render();
    refresh();
    const timer = live ? setInterval(refresh, options.pollMs || 1500) : null;
    return { refresh, destroy: () => timer && clearInterval(timer) };
  }

  root.KvDashboard = { mount, liveAdapter, simulationAdapter };
})(typeof window !== 'undefined' ? window : globalThis);
