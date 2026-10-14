// AstrBot Photon iMessage — minimal spectrum-ts sidecar.
//
// Photon.codes managed Spectrum only ships a TypeScript SDK (gRPC-based);
// there is no Python SDK and no public HTTP message API. This tiny Node
// bridge lets the AstrBot plugin (Python) talk to Spectrum over localhost
// HTTP:
//
//   Inbound (Spectrum -> AstrBot):
//     GET /inbound  -> NDJSON stream, one JSON event per line; blank lines
//                      are heartbeats. Single consumer at a time.
//   Outbound (AstrBot -> Spectrum):
//     POST /healthz           -> {"ok": true}
//     POST /send              -> {"ok": true, "messageId": "..."}
//         body {"spaceId","text","format":"text"|"markdown"}
//     POST /send-attachment   -> {"ok": true, "messageId": "..."}
//         body {"spaceId","path","kind":"attachment"|"voice"}
//     POST /react             -> {"ok": true}
//         body {"spaceId","messageId","emoji"}
//     POST /typing            -> {"ok": true}
//         body {"spaceId","state":"start"|"stop"}
//     POST /shutdown          -> {"ok": true}; then process exits.
//
// All requests except /healthz must carry `X-Sidecar-Token: <token>`.
//
// Env:
//   PHOTON_PROJECT_ID / PHOTON_PROJECT_SECRET   (Spectrum credentials)
//   SIDECAR_PORT (default 8789), SIDECAR_TOKEN, SIDECAR_BIND (127.0.0.1)
import http from "node:http";
import crypto from "node:crypto";

const projectId = process.env.PHOTON_PROJECT_ID;
const projectSecret = process.env.PHOTON_PROJECT_SECRET;
const port = Number(process.env.SIDECAR_PORT || "8789");
const bind = process.env.SIDECAR_BIND || "127.0.0.1";
const token = process.env.SIDECAR_TOKEN || "";

if (!projectId || !projectSecret || !token) {
  console.error(
    "photon-sidecar: PHOTON_PROJECT_ID, PHOTON_PROJECT_SECRET and SIDECAR_TOKEN are required"
  );
  process.exit(1);
}

// ------------------------------------------------------------------ SDK
let Spectrum, imessage;
let attachment, voice, spectrumText, spectrumMarkdown, spectrumTyping;
try {
  ({
    Spectrum,
    attachment,
    voice,
    text: spectrumText,
    markdown: spectrumMarkdown,
    typing: spectrumTyping,
  } = await import("spectrum-ts"));
  ({ imessage } = await import("spectrum-ts/providers/imessage"));
} catch (e) {
  console.error("photon-sidecar: failed to import spectrum-ts: " + (e?.stack || e));
  process.exit(1);
}

let app;
try {
  app = await Spectrum({
    projectId,
    projectSecret,
    providers: [imessage.config()],
    options: { flattenGroups: true },
  });
  console.error("photon-sidecar: Spectrum connected (project " + projectId + ")");
} catch (e) {
  console.error("photon-sidecar: Spectrum connect failed: " + (e?.stack || e));
  process.exit(1);
}

// ------------------------------------------------------------------ helpers
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"));
      } catch (e) {
        reject(e);
      }
    });
    req.on("error", reject);
  });
}

function ok(res, data = {}) {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ ok: true, ...data }));
}

function err(res, code = 400, message = "error") {
  res.writeHead(code, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ ok: false, error: { code, message } }));
}

function authed(req) {
  if (!token) return true;
  const got = req.headers["x-sidecar-token"];
  if (typeof got !== "string" || !got) return false;
  const a = Buffer.from(got);
  const b = Buffer.from(token);
  // timingSafeEqual 在长度不同时会抛 RangeError，必须先比较长度
  if (a.length !== b.length) return false;
  try {
    return crypto.timingSafeEqual(a, b);
  } catch {
    return false;
  }
}

// Resolve a space id to a live imessage space object.
async function resolveSpace(spaceId) {
  const im = imessage(app);
  const space = await im.space.get(spaceId);
  if (!space) throw new Error("unable to resolve space id " + spaceId);
  return space;
}

// ------------------------------------------------------------------ inbound
// Normalize a spectrum message into a compact JSON event for Python.
async function normalizeEvent(space, message) {
  const c = message.content || {};
  const msgSpace = message.space || {};
  const base = {
    messageId: message.id ?? null,
    platform: message.platform || "iMessage",
    space: {
      id: space.id ?? msgSpace.id ?? null,
      type: space.type ?? msgSpace.type ?? "dm",
      phone: space.phone ?? msgSpace.phone ?? null,
    },
    sender: { id: message.sender ? message.sender.id : null },
    direction: message.direction ?? null,
    timestamp:
      message.timestamp instanceof Date
        ? message.timestamp.toISOString()
        : message.timestamp ?? null,
  };
  switch (c.type) {
    case "text":
      return { ...base, content: { type: "text", text: c.text || "" } };
    case "attachment": {
      const info = {
        type: "attachment",
        name: c.name || null,
        mimeType: c.mimeType || c.mime_type || null,
        size: typeof c.size === "number" ? c.size : null,
      };
      // Inline small binary payloads as base64 when available.
      const bytes =
        c.data ?? c.payload ?? (typeof c.getData === "function" ? await c.getData() : null);
      if (bytes) {
        const buf = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
        if (buf.length <= MAX_INLINE) {
          info.dataBase64 = buf.toString("base64");
        } else {
          info.size = buf.length;
        }
      } else if (c.url) {
        info.url = c.url;
      }
      return { ...base, content: info };
    }
    default:
      return { ...base, content: { type: c.type || "unknown" } };
  }
}

const MAX_INLINE = Number(process.env.MAX_INLINE_ATTACHMENT_BYTES) || 5 * 1024 * 1024;

// ------------------------------------------------------------------ server
let consumerRes = null;

function deliver(line) {
  // 若当前无消费者连接（例如 AstrBot 尚未连上 / 正在重连），直接丢弃。
  // 插件的入站循环会持续保持一条连接，因此实际丢包只发生在启动瞬间。
  if (consumerRes && !consumerRes.writableEnded) {
    consumerRes.write(line + "\n");
  }
}

// Inbound: pull from app.messages and stream normalized JSON to the consumer.
const inbound = (async () => {
  for (;;) {
    try {
      for await (const [space, message] of app.messages) {
        if (message && message.direction && message.direction !== "inbound") continue;
        const ev = await normalizeEvent(space, message);
        if (ev) deliver(JSON.stringify(ev));
      }
      console.error("photon-sidecar: inbound stream ended — re-subscribing");
    } catch (e) {
      console.error("photon-sidecar: inbound stream error — re-subscribing: " + (e?.message || e));
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
})();

const server = http.createServer(async (req, res) => {
  const url = req.url || "";
  try {
    if (req.method === "GET" && url === "/inbound") {
      res.writeHead(200, {
        "Content-Type": "application/x-ndjson",
        "X-Accel-Buffering": "no",
        "Cache-Control": "no-cache",
      });
      consumerRes = res;
      const heartbeat = setInterval(() => {
        if (!res.writableEnded) res.write("\n");
      }, 25000);
      // 保持连接直到消费方断开。
      await new Promise((resolve) => {
        req.on("close", resolve);
        res.on("close", resolve);
      });
      clearInterval(heartbeat);
      if (consumerRes === res) consumerRes = null;
      return;
    }

    if (url === "/healthz") {
      return ok(res);
    }

    if (!authed(req)) {
      return err(res, 401, "unauthorized");
    }

    if (req.method === "POST" && url === "/send") {
      const { spaceId, text, format = "text" } = await readBody(req);
      if (!spaceId || typeof text !== "string") return err(res, 400, "spaceId and text required");
      const builder =
        format === "markdown" ? spectrumMarkdown(text) : spectrumText(text);
      const space = await resolveSpace(spaceId);
      const result = await space.send(builder);
      return ok(res, { messageId: result?.id || null });
    }

    if (req.method === "POST" && url === "/send-attachment") {
      const { spaceId, path, kind = "attachment" } = await readBody(req);
      if (!spaceId || !path) return err(res, 400, "spaceId and path required");
      const space = await resolveSpace(spaceId);
      const builder = kind === "voice" ? voice(path) : attachment(path);
      const result = await space.send(builder);
      return ok(res, { messageId: result?.id || null });
    }

    if (req.method === "POST" && url === "/react") {
      const { spaceId, messageId, emoji } = await readBody(req);
      if (!spaceId || !messageId || !emoji) return err(res, 400, "spaceId, messageId, emoji required");
      const space = await resolveSpace(spaceId);
      await space.react(space.getMessage ? space.getMessage(messageId) : messageId, emoji);
      return ok(res);
    }

    if (req.method === "POST" && url === "/typing") {
      const { spaceId, state } = await readBody(req);
      if (!spaceId || !["start", "stop"].includes(state)) {
        return err(res, 400, "state must be start|stop");
      }
      const space = await resolveSpace(spaceId);
      // spectrum-ts 通过 typing() 内容构造器 + space.send() 发送输入状态
      await space.send(spectrumTyping(state));
      return ok(res);
    }

    if (req.method === "POST" && url === "/shutdown") {
      ok(res);
      setImmediate(async () => {
        try { await app.stop(); } catch (_) {}
        process.exit(0);
      });
      return;
    }

    return err(res, 404, "not found");
  } catch (e) {
    return err(res, 500, (e && e.message) || String(e));
  }
});

server.listen(port, bind, () => {
  console.error(
    `photon-sidecar: listening on http://${bind}:${port}` +
      (token ? " (token auth enabled)" : " (WARNING: no token)")
  );
});

// Keep the inbound loop referenced (it may be a floating promise otherwise).
void inbound;

// ------------------------------------------------------------------ 孤儿保护
// AstrBot 通过 stdin=PIPE 启动本进程。父进程一旦消失（含被 SIGKILL），
// 我们这里的 stdin 会收到 EOF —— 此时主动退出，避免 sidecar 残留并占住端口，
// 导致下次启动失败。
function shutdown(reason) {
  console.error("photon-sidecar: shutting down (" + reason + ")");
  Promise.resolve()
    .then(() => app.stop())
    .catch(() => {})
    .finally(() => process.exit(0));
}

process.stdin.on("end", () => shutdown("stdin EOF (parent exited)"));
process.stdin.on("close", () => shutdown("stdin closed"));
process.stdin.resume();
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));