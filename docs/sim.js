/*
 * DistribuKV in the browser: a simulation of the cluster for the project page.
 *
 * Placement is computed exactly like the Java code (MD5 -> signed 64-bit tokens, 256 virtual
 * nodes per node, first N distinct nodes clockwise), so replica lists match the real cluster.
 * Quorum rules, last-write-wins, hints, read repair and the Kafka log follow the same logic as
 * Coordinator.java, without networking or timing.
 */
(function (root) {
  'use strict';

  // ---- MD5 (browsers' SubtleCrypto has no MD5) ------------------------------------------------
  const S = [7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
    5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20,
    4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
    6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21];
  const K = Array.from({ length: 64 }, (_, i) => Math.floor(Math.abs(Math.sin(i + 1)) * 4294967296) >>> 0);

  function md5(bytes) {
    const len = bytes.length;
    const blocks = ((len + 8) >>> 6) + 1;
    const words = new Uint32Array(blocks * 16);
    for (let i = 0; i < len; i++) words[i >>> 2] |= bytes[i] << ((i % 4) * 8);
    words[len >>> 2] |= 0x80 << ((len % 4) * 8);
    words[blocks * 16 - 2] = (len * 8) >>> 0;
    let a0 = 0x67452301, b0 = 0xefcdab89, c0 = 0x98badcfe, d0 = 0x10325476;
    for (let b = 0; b < blocks; b++) {
      let A = a0, B = b0, C = c0, D = d0;
      for (let i = 0; i < 64; i++) {
        let F, g;
        if (i < 16) { F = (B & C) | (~B & D); g = i; }
        else if (i < 32) { F = (D & B) | (~D & C); g = (5 * i + 1) % 16; }
        else if (i < 48) { F = B ^ C ^ D; g = (3 * i + 5) % 16; }
        else { F = C ^ (B | ~D); g = (7 * i) % 16; }
        F = (F + A + K[i] + words[b * 16 + g]) >>> 0;
        A = D; D = C; C = B;
        B = (B + ((F << S[i]) | (F >>> (32 - S[i])))) >>> 0;
      }
      a0 = (a0 + A) >>> 0; b0 = (b0 + B) >>> 0; c0 = (c0 + C) >>> 0; d0 = (d0 + D) >>> 0;
    }
    const out = new Uint8Array(16);
    [a0, b0, c0, d0].forEach((v, i) => { for (let j = 0; j < 4; j++) out[i * 4 + j] = (v >>> (8 * j)) & 255; });
    return out;
  }

  /** ConsistentHashRing.hash: first 8 bytes of MD5, big-endian, as a signed long. */
  function hash(str) {
    const digest = md5(new TextEncoder().encode(str));
    let h = 0n;
    for (let i = 0; i < 8; i++) h = (h << 8n) | BigInt(digest[i]);
    return BigInt.asIntN(64, h);
  }

  class Ring {
    constructor(nodeIds, virtualNodes) {
      const tokens = new Map();
      for (const id of nodeIds) for (let i = 0; i < virtualNodes; i++) tokens.set(hash(id + '#' + i), id);
      this.tokens = [...tokens.entries()].sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
      this.size = nodeIds.length;
    }

    preferenceList(key, n) {
      const wanted = Math.min(n, this.size);
      const position = hash(key);
      let lo = 0, hi = this.tokens.length;
      while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        if (this.tokens[mid][0] < position) lo = mid + 1; else hi = mid;
      }
      const result = [];
      for (let k = 0; k < this.tokens.length && result.length < wanted; k++) {
        const id = this.tokens[(lo + k) % this.tokens.length][1];
        if (!result.includes(id)) result.push(id);
      }
      return result;
    }
  }

  const newer = (a, b) => !b || a.timestamp > b.timestamp || (a.timestamp === b.timestamp && a.nodeId > b.nodeId);
  const PARTITIONS = 6;

  class Cluster {
    constructor(ids = ['node1', 'node2', 'node3', 'node4', 'node5'], replicationFactor = 3, virtualNodes = 256) {
      this.ids = ids;
      this.n = replicationFactor;
      this.ring = new Ring(ids, virtualNodes);
      this.nodes = new Map(ids.map(id => [id, { id, state: 'UP', store: new Map(), hints: [], logOffset: 0 }]));
      this.log = [];
      this.partitionOffsets = new Array(PARTITIONS).fill(0);
      this.lastTick = 0;
    }

    replicasFor(key) { return this.ring.preferenceList(key, this.n); }
    isUp(id) { return this.nodes.get(id).state === 'UP'; }
    liveKeys(id) { return [...this.nodes.get(id).store.values()].filter(v => !v.tombstone).length; }
    tick() { this.lastTick = Math.max(Date.now(), this.lastTick + 1); return this.lastTick; }

    put(coordinator, key, value, consistency) {
      return this.write(coordinator, key, { value, timestamp: this.tick(), nodeId: coordinator, tombstone: false }, consistency);
    }

    delete(coordinator, key, consistency) {
      return this.write(coordinator, key, { value: null, timestamp: this.tick(), nodeId: coordinator, tombstone: true }, consistency);
    }

    write(coordinator, key, version, consistency) {
      if (!this.isUp(coordinator)) return this.unreachable(coordinator);
      const events = [];
      const replicas = this.replicasFor(key);
      const required = consistency === 'STRONG' ? Math.floor(replicas.length / 2) + 1 : 1;
      const acked = [];
      for (const replica of replicas) {
        if (this.isUp(replica)) {
          this.apply(replica, key, version);
          acked.push(replica);
        } else {
          this.nodes.get(coordinator).hints.push({ target: replica, key, version });
          events.push(`${replica} is unreachable: ${coordinator} stored a hint for it`);
        }
      }
      const position = this.append(key, version);
      events.push(`appended to the Kafka log at ${position}`);
      this.consumeAll();

      if (consistency === 'EVENTUAL') {
        return {
          status: 200, events,
          body: { key, version: version.timestamp, consistency, coordinator, replicas, acknowledgedBy: acked, requiredAcks: 0, replicationLog: position }
        };
      }
      if (acked.length >= required) {
        return {
          status: 200, events,
          body: { key, version: version.timestamp, consistency, coordinator, replicas, acknowledgedBy: acked, requiredAcks: required, replicationLog: null }
        };
      }
      events.push('not rolled back: replicas that stored it keep it (same as Dynamo/Cassandra)');
      return {
        status: 503, events,
        body: { error: 'quorum_not_reached', message: `write needed ${required} replica responses but got ${acked.length} [${acked.join(', ')}]`, required, respondedBy: acked }
      };
    }

    get(coordinator, key, consistency) {
      if (!this.isUp(coordinator)) return this.unreachable(coordinator);
      const events = [];
      const replicas = this.replicasFor(key);
      const required = consistency === 'STRONG' ? Math.floor(replicas.length / 2) + 1 : 1;
      const responders = replicas.filter(id => this.isUp(id));
      if (responders.length < required) {
        return {
          status: 503, events,
          body: { error: 'quorum_not_reached', message: `read needed ${required} replica responses but got ${responders.length} [${responders.join(', ')}]`, required, respondedBy: responders }
        };
      }
      const counted = responders.slice(0, required);
      let newest = null;
      for (const id of counted) {
        const v = this.nodes.get(id).store.get(key);
        if (v && newer(v, newest)) newest = v;
      }
      // Read repair runs once all replicas answered; every reachable replica is compared.
      let repairNewest = newest;
      for (const id of responders) {
        const v = this.nodes.get(id).store.get(key);
        if (v && newer(v, repairNewest)) repairNewest = v;
      }
      if (repairNewest) {
        for (const id of responders) {
          const v = this.nodes.get(id).store.get(key);
          if (!v || newer(repairNewest, v)) {
            this.apply(id, key, repairNewest);
            events.push(`read repair: pushed the newest version to stale replica ${id}`);
          }
        }
      }
      const found = !!newest && !newest.tombstone;
      return {
        status: found ? 200 : 404, events,
        body: { key, value: found ? newest.value : null, version: newest ? newest.timestamp : 0, found, consistency, coordinator, replicas, respondedBy: counted, requiredResponses: required }
      };
    }

    /** state: UP | CRASHED | ISOLATED */
    setState(id, state) {
      const node = this.nodes.get(id);
      const before = node.state;
      if (before === state) return [];
      node.state = state;
      const events = [];
      if (state === 'CRASHED') {
        events.push(`${id} crashed (its MySQL data survives)`);
        if (node.hints.length) {
          events.push(`${id} lost ${node.hints.length} in-memory hint(s); the Kafka log still has those writes`);
          node.hints = [];
        }
      } else if (state === 'ISOLATED') {
        events.push(`${id} is partitioned: no client or peer traffic, Redis heartbeats and Kafka consumption paused`);
      } else {
        events.push(before === 'CRASHED' ? `${id} restarted and is UP` : `${id} partition healed`);
      }
      events.push(...this.deliverHints(), ...this.consumeAll());
      return events;
    }

    deliverHints() {
      const events = [];
      for (const holder of this.nodes.values()) {
        if (holder.state !== 'UP' || !holder.hints.length) continue;
        const delivered = {};
        holder.hints = holder.hints.filter(h => {
          if (!this.isUp(h.target)) return true;
          this.apply(h.target, h.key, h.version);
          delivered[h.target] = (delivered[h.target] || 0) + 1;
          return false;
        });
        for (const [target, count] of Object.entries(delivered)) {
          events.push(`hinted handoff: ${holder.id} delivered ${count} write(s) to ${target}`);
        }
      }
      return events;
    }

    consumeAll() {
      const events = [];
      for (const node of this.nodes.values()) {
        if (node.state !== 'UP') continue;
        let applied = 0;
        for (; node.logOffset < this.log.length; node.logOffset++) {
          const entry = this.log[node.logOffset];
          if (this.replicasFor(entry.key).includes(node.id) && this.apply(node.id, entry.key, entry.version)) applied++;
        }
        if (applied) events.push(`${node.id} caught up ${applied} write(s) from the Kafka log`);
      }
      return events;
    }

    apply(id, key, version) {
      const store = this.nodes.get(id).store;
      if (!newer(version, store.get(key))) return false;
      store.set(key, version);
      return true;
    }

    append(key, version) {
      const partition = Number(BigInt.asUintN(64, hash(key)) % BigInt(PARTITIONS));
      const offset = this.partitionOffsets[partition]++;
      this.log.push({ key, version });
      return `kafka:kv-replication/${partition}@${offset}`;
    }

    unreachable(coordinator) {
      const state = this.nodes.get(coordinator).state;
      return {
        status: state === 'ISOLATED' ? 503 : 0, events: [],
        body: { error: 'coordinator_unreachable', message: `${coordinator} is ${state === 'ISOLATED' ? 'isolated' : 'down'}; send the request to another node` }
      };
    }
  }

  root.KvSim = { md5, hash, Ring, Cluster };
})(typeof window !== 'undefined' ? window : globalThis);
