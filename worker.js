// 互传远程中转：只做“谁在线”和“有新消息通知”。
// 消息内容在手机上加密后存到网盘里，这里看不到内容，也不存内容（只暂存几十字节的通知）。
//
// 手机 ←WebSocket→ 这个 Worker（Durable Object）←WebSocket→ 对方手机
// 对方不在线时，通知暂存 7 天，对方上线后补发。

const DEVICE = /^[A-Za-z0-9_-]{1,64}$/;
const PENDING_TTL_MS = 7 * 24 * 3600 * 1000;
const PENDING_MAX_PER_DEVICE = 200;

function same(a, b) {
  // 常量时间比较，避免靠响应时间猜口令
  if (typeof a !== "string" || typeof b !== "string" || a.length !== b.length) return false;
  let r = 0;
  for (let i = 0; i < a.length; i++) r |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return r === 0;
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (!env.SECRET) {
      return json({ ok: false, error: "服务器还没有设置口令（SECRET）" }, 500);
    }
    if (!same(req.headers.get("X-Relay-Secret") || "", env.SECRET)) {
      return json({ ok: false, error: "口令不对" }, 401);
    }
    if (url.pathname === "/test") {
      return json({ ok: true, time: Date.now() });
    }
    if (url.pathname === "/ws") {
      const dev = req.headers.get("X-Device") || "";
      if (!DEVICE.test(dev)) return json({ ok: false, error: "设备 ID 格式不对" }, 400);
      if (req.headers.get("Upgrade") !== "websocket") return json({ ok: false, error: "需要 WebSocket" }, 426);
      const stub = env.HUB.get(env.HUB.idFromName("main"));
      return stub.fetch(req);
    }
    return json({ ok: false, error: "没有这个地址" }, 404);
  },
};

function json(o, status = 200) {
  return new Response(JSON.stringify(o), { status, headers: { "content-type": "application/json; charset=utf-8" } });
}

export class Hub {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.sql = ctx.storage.sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS pending (id INTEGER PRIMARY KEY AUTOINCREMENT, to_dev TEXT NOT NULL, body TEXT NOT NULL, ts INTEGER NOT NULL)"
    );
    // 心跳 ping/pong 由平台自动应答，不会唤醒休眠中的对象，也不计费
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  online() {
    const set = new Set();
    for (const ws of this.ctx.getWebSockets()) {
      const t = this.ctx.getTags(ws);
      if (t.length) set.add(t[0]);
    }
    return [...set];
  }

  async fetch(req) {
    const dev = req.headers.get("X-Device");
    // 同一设备重复连接：踢掉旧的
    for (const old of this.ctx.getWebSockets(dev)) {
      try { old.close(1000, "被同一设备的新连接替换"); } catch (_) {}
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server, [dev]);

    // 补发离线期间的通知
    const cutoff = Date.now() - PENDING_TTL_MS;
    this.sql.exec("DELETE FROM pending WHERE ts < ?", cutoff);
    const rows = this.sql.exec("SELECT body FROM pending WHERE to_dev = ? ORDER BY id", dev).toArray();
    for (const r of rows) server.send(r.body);
    this.sql.exec("DELETE FROM pending WHERE to_dev = ?", dev);

    server.send(JSON.stringify({ op: "presence", online: this.online() }));
    this.broadcast({ op: "presence", online: this.online() }, dev);
    return new Response(null, { status: 101, webSocket: client });
  }

  broadcast(msg, exceptDev) {
    const s = JSON.stringify(msg);
    for (const ws of this.ctx.getWebSockets()) {
      const t = this.ctx.getTags(ws);
      if (t[0] === exceptDev) continue;
      try { ws.send(s); } catch (_) {}
    }
  }

  webSocketMessage(ws, message) {
    if (typeof message !== "string" || message.length > 2048) return;
    const from = this.ctx.getTags(ws)[0];
    let m;
    try { m = JSON.parse(message); } catch (_) { return; }
    if (m.op !== "notify" || typeof m.to !== "string" || !DEVICE.test(m.to)) return;
    const body = JSON.stringify({ op: "notify", from, body: m.body || {} });
    const targets = this.ctx.getWebSockets(m.to);
    let delivered = false;
    for (const t of targets) {
      try { t.send(body); delivered = true; } catch (_) {}
    }
    if (!delivered) {
      const n = this.sql.exec("SELECT COUNT(*) AS n FROM pending WHERE to_dev = ?", m.to).one().n;
      if (n >= PENDING_MAX_PER_DEVICE) {
        ws.send(JSON.stringify({ op: "error", mid: m.body && m.body.mid, error: "对方离线且暂存的通知已满" }));
        return;
      }
      this.sql.exec("INSERT INTO pending (to_dev, body, ts) VALUES (?, ?, ?)", m.to, body, Date.now());
    }
    ws.send(JSON.stringify({ op: "sent", mid: m.body && m.body.mid, online: delivered }));
  }

  webSocketClose(ws, code, reason) {
    try { ws.close(code, reason); } catch (_) {}
    this.broadcast({ op: "presence", online: this.online() }, null);
  }

  webSocketError(ws) {
    this.webSocketClose(ws, 1011, "error");
  }
}
