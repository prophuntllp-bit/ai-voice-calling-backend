const express = require("express");
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const https = require("https");
const WebSocket = require("ws");
const axios = require("axios");
const FormData = require("form-data");
const Redis = require("ioredis");
const { Counter, Histogram, Registry, collectDefaultMetrics } = require("prom-client");
const { LanguageManager } = require("./language-manager");
const { v2: cloudinary } = require("cloudinary");
const { AgniBridge, createAgniSession } = require("./agni-bridge");

if (process.env.CLOUDINARY_CLOUD_NAME) {
  cloudinary.config({
    cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
    api_key: process.env.CLOUDINARY_API_KEY,
    api_secret: process.env.CLOUDINARY_API_SECRET,
  });
}

const config = {
  port: parseInt(process.env.PORT || "8000", 10),
  services: {
    vad: process.env.VAD_URL || "http://vad:8001",
    stt: process.env.STT_URL || "http://stt:8002",
    tts: process.env.TTS_URL || "http://tts:8003",
    llm: process.env.LLM_URL || "http://llm:11434",
    crmAdapter: process.env.CRM_ADAPTER_URL || "http://crm-adapter:8010",
    knowledge: process.env.KNOWLEDGE_SERVICE_URL || "http://knowledge-service:8011",
    platformApi: process.env.PLATFORM_API_URL || "http://platform-api:8013",
  },
  redisUrl: process.env.REDIS_URL || "redis://redis:6379",
  internalToken: process.env.ORCHESTRATOR_INTERNAL_TOKEN || "local-dev-internal-token",
  recordingsDir: process.env.RECORDINGS_DIR || "/data/recordings",
  maxConcurrentCalls: parseInt(process.env.MAX_CONCURRENT || "50", 10),
  callTimeoutMs: parseInt(process.env.CALL_TIMEOUT_MS || `${5 * 60 * 1000}`, 10),
  sttTimeoutMs: parseInt(process.env.STT_REQUEST_TIMEOUT_MS || "45000", 10),
  enablex: {
    appId: process.env.ENABLEX_APP_ID || "",
    appKey: process.env.ENABLEX_APP_KEY || "",
    fromNumber: process.env.ENABLEX_FROM_NUMBER || "",
    baseUrl: (process.env.ENABLEX_VOICE_BASE_URL || "https://api.enablex.io/voice/v1").replace(/\/$/, ""),
  },
  telephonyProvider: (process.env.TELEPHONY_PROVIDER || "enablex").toLowerCase(),
  // Ravan.ai Agni — set both vars to enable; leave blank to use local STT/LLM/TTS
  agni: {
    apiKey: process.env.AGNI_API_KEY || "",
    agentId: process.env.AGNI_AGENT_ID || "",
    get enabled() { return !!(this.apiKey && this.agentId); },
  },
};

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const registry = new Registry();
collectDefaultMetrics({ register: registry });

const callsTotal = new Counter({
  name: "calls_total",
  help: "Total number of calls handled",
  labelNames: ["status"],
  registers: [registry],
});

const callDuration = new Histogram({
  name: "call_duration_seconds",
  help: "End to end call duration",
  buckets: [5, 15, 30, 60, 120, 300],
  registers: [registry],
});

const serviceLatency = new Histogram({
  name: "service_latency_ms",
  help: "Latency by dependency",
  labelNames: ["service"],
  buckets: [25, 50, 100, 250, 500, 1000, 3000, 5000],
  registers: [registry],
});

const redis = new Redis(config.redisUrl, { lazyConnect: false, maxRetriesPerRequest: 2 });
redis.on("error", (err) => console.error("[redis] connection error:", err.message));
const sessions = new Map();
const languageManager = new LanguageManager();
let acceptingTraffic = true;

// ---------------------------------------------------------------------------
// Live call feed — broadcast transcript events to dashboard WebSocket clients
// ---------------------------------------------------------------------------
function broadcastLiveEvent(session, event) {
  if (!session?.liveSubscribers?.size) return;
  const payload = JSON.stringify({ ...event, callSid: session.callSid, timestamp: Date.now() });
  for (const sub of session.liveSubscribers) {
    if (sub.readyState === WebSocket.OPEN) {
      try { sub.send(payload); } catch (_) {}
    }
  }
}
const enablexAuthHeader = config.enablex.appId && config.enablex.appKey
  ? `Basic ${Buffer.from(`${config.enablex.appId}:${config.enablex.appKey}`).toString("base64")}`
  : "";

fs.mkdirSync(config.recordingsDir, { recursive: true });

app.use(express.json({ limit: "5mb" }));
app.use(express.urlencoded({ extended: true }));

// CORS — allow all origins; restrict via ALLOWED_ORIGINS env var when ready
app.use((req, res, next) => {
  const origin = req.headers.origin || '*';
  res.header('Access-Control-Allow-Origin', origin);
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Internal-Token');
  res.header('Access-Control-Allow-Credentials', 'true');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// Auth middleware — verifies X-Internal-Token on protected routes
function requireToken(req, res, next) {
  const token = req.headers['x-internal-token'] || '';
  if (!token || token !== config.internalToken) {
    console.warn(`[auth] 401 on ${req.method} ${req.path} — token present: ${!!token}, expected: ${config.internalToken.slice(0, 6)}***`);
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}
// Recordings endpoint — Redis-first so files survive container restarts / redeploys.
// Falls back to local disk for files written this session that haven't been cached yet.
app.get("/recordings/:callSid/mixed.wav", async (req, res) => {
  const { callSid } = req.params;
  try {
    const b64 = await redis.get(`recording:${callSid}`);
    if (b64) {
      const buf = Buffer.from(b64, "base64");
      res.set("Content-Type", "audio/wav");
      res.set("Content-Length", buf.length);
      res.set("Cache-Control", "public, max-age=86400");
      return res.send(buf);
    }
  } catch { /* fall through to disk */ }
  // Disk fallback (works within the same container session)
  const diskPath = path.join(config.recordingsDir, safeRecordingId(callSid), "mixed.wav");
  if (fs.existsSync(diskPath)) return res.sendFile(diskPath);
  return res.status(404).json({ error: "Recording not found" });
});
// Serve other recording files (caller.wav, agent.wav, timeline.json) from disk
app.use("/recordings", express.static(config.recordingsDir));

function getPublicBaseUrl(req) {
  const host = process.env.PUBLIC_HOST || req.get("host") || "localhost:8000";
  const protocol = req.get("x-forwarded-proto") || (host.includes("localhost") ? "http" : "https");
  return `${protocol}://${host}`.replace(/\/$/, "");
}

function getPublicWsBaseUrl(req) {
  return getPublicBaseUrl(req).replace(/^http/i, "ws");
}

function getConfiguredPublicBaseUrl() {
  const host = process.env.PUBLIC_HOST || `localhost:${config.port}`;
  const protocol = host.includes("localhost") ? "http" : "https";
  return `${protocol}://${host}`.replace(/\/$/, "");
}

function getConfiguredPublicWsBaseUrl() {
  return getConfiguredPublicBaseUrl().replace(/^http/i, "ws");
}

function resolveTelephonyProvider(requestedProvider) {
  const provider = String(requestedProvider || config.telephonyProvider || "enablex").trim().toLowerCase();
  return provider === "enablex" ? "enablex" : "simulated";
}

function hasEnablexConfig() {
  return Boolean(enablexAuthHeader && config.enablex.fromNumber);
}

function buildEnablexOpeningLine(leadName = "there", agentName = "Priya", companyName = "Prophunt", projectName = "") {
  const proj = projectName ? `${projectName} mein` : "hamare project mein";
  return `Namaste ${leadName} ji! Main ${agentName} bol rahi hoon ${companyName} se. Aapne ${proj} interest dikhaya tha — kya abhi do minute hain?`;
}

function normalizeEnablexPhoneNumber(phone) {
  const digits = String(phone || "").replace(/\D/g, "");
  if (digits.length === 10) return `91${digits}`;
  return digits;
}

async function placeEnablexOutboundCall({ lead, session, openingLine }) {
  if (!hasEnablexConfig()) {
    throw new Error("EnableX credentials or caller number are missing");
  }

  const publicBaseUrl = getConfiguredPublicBaseUrl();
  const payload = {
    name: "Prophunt AI Voice Agent",
    owner_ref: session.callSid,
    auto_record: false,
    from: normalizeEnablexPhoneNumber(config.enablex.fromNumber),
    to: normalizeEnablexPhoneNumber(lead.phone),
    event_url: `${publicBaseUrl}/call/enablex/events`,
  };

  let response;
  try {
    response = await timed("enablex", () =>
      axios.post(`${config.enablex.baseUrl}/call`, payload, {
        headers: {
          Authorization: enablexAuthHeader,
          "Content-Type": "application/json",
        },
        timeout: 45000,
      })
    );
  } catch (error) {
    console.error("[enablex] outbound call failed", {
      message: error.message,
      status: error.response?.status,
      data: error.response?.data,
      from: payload.from,
      to: payload.to,
    });
    throw error;
  }

  const data = response.data || {};
  console.log("[enablex] outbound call response", {
    status: response.status,
    voice_id: data.voice_id,
    state: data.state,
    msg: data.msg,
  });
  return {
    provider_call_id: data.voice_id || data.call_id || data.callId || data.id || data.sid || session.callSid,
    provider_status: data.state || data.status || "initiated",
    raw: data,
  };
}

async function callEnablexApi(method, pathName, payload = null, options = {}) {
  if (!enablexAuthHeader) {
    throw new Error("EnableX credentials are missing");
  }
  const response = await timed("enablex", () =>
    axios({
      method,
      url: `${config.enablex.baseUrl}${pathName}`,
      data: payload,
      headers: {
        Authorization: enablexAuthHeader,
        "Content-Type": "application/json",
      },
      timeout: options.timeout || 45000,
    })
  );
  const data = response.data;
  if (
    data &&
    (data.statusCode >= 400 ||
      data.result >= 400 ||
      /not found|not allowed|failed|error/i.test(String(data.msg || data.playstate || data.state || "")))
  ) {
    const error = new Error(data.msg || data.playstate || data.state || "EnableX API rejected the request");
    error.response = { status: data.statusCode || data.result || response.status, data };
    throw error;
  }
  return data;
}

async function callEnablexDeleteRaw(pathName) {
  if (!enablexAuthHeader) {
    throw new Error("EnableX credentials are missing");
  }
  const endpoint = new URL(`${config.enablex.baseUrl}${pathName}`);
  return timed("enablex", () =>
    new Promise((resolve, reject) => {
      const req = https.request(
        {
          protocol: endpoint.protocol,
          hostname: endpoint.hostname,
          port: endpoint.port || 443,
          path: `${endpoint.pathname}${endpoint.search}`,
          method: "DELETE",
          headers: {
            Authorization: enablexAuthHeader,
            "Content-Type": "application/json",
          },
        },
        (res) => {
          let body = "";
          res.on("data", (chunk) => {
            body += chunk;
          });
          res.on("end", () => {
            if (res.statusCode >= 200 && res.statusCode < 300) {
              resolve(body);
              return;
            }
            const error = new Error(`EnableX delete failed with status ${res.statusCode}`);
            error.response = { status: res.statusCode, data: body };
            reject(error);
          });
        }
      );
      req.on("error", reject);
      req.end("");
    })
  );
}

async function startEnablexStream(voiceId) {
  const wssHost = `${getConfiguredPublicWsBaseUrl()}/audio/enablex/${encodeURIComponent(voiceId)}`;
  console.log("[enablex-media] starting stream", { voice_id: voiceId, wss_host: wssHost });
  return callEnablexApi(
    "put",
    `/call/${encodeURIComponent(voiceId)}/stream`,
    { wss_host: wssHost },
    { timeout: 10000 }
  );
}

async function stopEnablexStream(voiceId) {
  return callEnablexDeleteRaw(`/call/${encodeURIComponent(voiceId)}/stream`);
}

async function hangupEnablexCall(voiceId) {
  return callEnablexDeleteRaw(`/call/${encodeURIComponent(voiceId)}`);
}

const ENABLEX_STREAM_READY_STATUSES = new Set([
  "answered",
  "answer",
  "connected",
  "in-progress",
  "in_progress",
  "live",
  "ongoing",
  "active",
  "bridged",
]);

function extractEnablexCallSid(payload = {}) {
  return payload.voice_id || payload.call_id || payload.callId || payload.id || payload.sid || payload.call_sid;
}

function normalizeEnablexStatus(payload = {}) {
  const rawStatus = payload.status || payload.state || payload.event || payload.call_status || payload.callStatus || "";
  return String(rawStatus).toLowerCase();
}

function shouldStartEnablexStream(callStatus) {
  return ENABLEX_STREAM_READY_STATUSES.has(String(callStatus || "").toLowerCase());
}

function scheduleEnablexStreamStart(session, reason = "scheduled", options = {}) {
  const force = options.force === true;
  if (!session?.callSid || session.closed || session.telephony?.streamStarted || (!force && session.telephony?.streamStartScheduled)) {
    return;
  }
  session.telephony = {
    ...(session.telephony || {}),
    provider: "enablex",
    streamStartScheduled: true,
    streamStartInFlight: false,
    streamStartReason: reason,
  };

  const voiceId = session.callSid;
  // post-dial: only 3 quick attempts (call may already be connected by the time we dial)
  // event-connected: single immediate attempt — EnableX is ready at this point
  const isPostDial = reason === "post-dial";
  const delays = isPostDial
    ? [0, 1500, 4000]           // 3 attempts only — event-connected handles the rest
    : [0, 1000, 3000, 6000, 10000, 15000, 21000, 28000]; // robust retry after connected event
  delays.forEach((delayMs, index) => {
    setTimeout(async () => {
      const current = sessions.get(voiceId);
      if (!current || current.closed || current.telephony?.streamStarted || current.telephony?.streamStartInFlight) return;
      try {
        current.telephony = {
          ...(current.telephony || {}),
          provider: "enablex",
          streamStartInFlight: true,
        };
        console.log("[enablex-media] stream start attempt", { voice_id: voiceId, attempt: index + 1, reason });
        const streamResponse = await startEnablexStream(voiceId);
        console.log("[enablex-media] stream start accepted", { voice_id: voiceId, attempt: index + 1, response: streamResponse });
        current.telephony = {
          ...(current.telephony || {}),
          provider: "enablex",
          streamStartResponse: streamResponse,
          streamStartInFlight: false,
          streamStartScheduled: false,
          streamStarted: true,
        };
        await persistSession(current);
      } catch (streamError) {
        const errorPayload = streamError.response?.data || streamError.message;
        // post-dial failures are expected — EnableX fires stream before call is answered
        console.log("[enablex-media] stream start failed (will retry)", {
          voice_id: voiceId,
          attempt: index + 1,
          reason,
          state: errorPayload?.state || "unknown",
        });
        current.telephony = {
          ...(current.telephony || {}),
          provider: "enablex",
          streamStartError: errorPayload,
          streamStartInFlight: false,
          streamStartScheduled: index < delays.length - 1,
        };
        await persistSession(current).catch(() => {});
      }
    }, delayMs);
  });
}

function nowIso() {
  return new Date().toISOString();
}

// Extract qualification facts from user transcripts in real-time.
// Called on every utterance so the agent always has up-to-date session memory.
function extractQualification(text, session) {
  if (!session.qualification) {
    session.qualification = { bhk: null, budget: null, purpose: null, timeline: null };
  }
  const q = session.qualification;
  const t = (text || "").toLowerCase();

  // ── Purpose: investment vs self-use ─────────────────────────────────────────
  // Covers: romanized Hinglish, phonetic STT variants, Devanagari, Marathi
  if (!q.purpose) {
    const investRx = /\b(?:invest(?:ment|ing)?|inwestment|invst|निवेश|गुंतवणूक|गुंतवणुकी|rental[\s-]?(?:yield|return|income|ke liye)?|rent(?:al)?[\s-]?(?:chahiye|ke liye|purpose)?|kiraya|किराया|किराए[\s-]?(?:के लिए|ke liye|sathi|साठी)|resale|re[\s-]?sale|return[\s-]?chahiye|appreciation|bhad[\s-]?vatila|passive[\s-]?income|renting|vikne[\s-]?sathi|बेचने[\s-]?के[\s-]?लिए|बेचना[\s-]?(?:hai|ahe)?|भाड्याने|बेचायचे|flipping|flip)\b/i;
    const selfUseRx = /\b(?:khud|self[\s-]?use|self[\s-]?yuz|self[\s-]?uz|सेल्फ[\s-]?यूज|खुद[\s-]?(?:ke liye|reh|rah|रहना|रहेंगे|rahna)?|apne[\s-]?liye|apna[\s-]?ghar|स्वयं|rehne[\s-]?ke[\s-]?liye|reh(?:na)?[\s-]?(?:hai|chahiye)?|rahen(?:ge)?|end[\s-]?use|खुद[\s-]?के[\s-]?लिए|खुद[\s-]?रहना|ghar[\s-]?chahiye|house[\s-]?chahiye|ghar[\s-]?ghyaycha|rahaycha[\s-]?ahe|settle(?:ment)?|family[\s-]?ke[\s-]?liye|personal[\s-]?use|स्वतःसाठी|राहायला|स्वतःसाठी|स्वत:[\s-]?साठी|rajniti)\b/i;
    if (investRx.test(t)) q.purpose = "investment";
    else if (selfUseRx.test(t)) q.purpose = "self-use";
  }

  // ── BHK preference ───────────────────────────────────────────────────────────
  // Handles: digits, English words, Hindi words, Marathi words, phonetic STT forms
  // (e.g. "टू बी एच के" = ElevenLabs STT output for spoken "2 BHK")
  if (!q.bhk) {
    const bhkM =
      // digit + BHK/bedroom
      text.match(/([1-4])\s*(?:BHK|बीएचके|बी\s*एच\s*के|bedroom|bhk|b\.?h\.?k)/i) ||
      // "teen / three / 3 / तीन / थ्री" BHK
      text.match(/(?:teen|tin|three|3|तीन|थ्री|तिन)\s*(?:BHK|bedroom|बीएचके|बी\s*एच\s*के|bhk)/i) ||
      // "do / two / 2 / दो / टू / don" BHK
      text.match(/(?:do|don|two|2|दो|टू)\s*(?:BHK|bedroom|बीएचके|बी\s*एच\s*के|bhk)/i) ||
      // "ek / one / 1 / एक / वन" BHK
      text.match(/(?:ek|one|1|एक|वन)\s*(?:BHK|bedroom|बीएचके|बी\s*एच\s*के|bhk)/i) ||
      // "char / four / 4 / चार / फोर" BHK
      text.match(/(?:char|chaar|four|4|चार|फोर|फ़ोर)\s*(?:BHK|bedroom|बीएचके|बी\s*एच\s*के|bhk)/i) ||
      // phonetic Devanagari from STT: "टू बी एच के" / "थ्री बी एच के"
      text.match(/(टू|वन|थ्री|फोर|फ़ोर)\s+बी\s+एच\s+के/i) ||
      // "teen / do / ek kamre / kamra" (Hindi room count without BHK keyword)
      text.match(/(?:teen|तीन|3)\s+(?:kamre?|कमरे?|room)/i) ||
      text.match(/(?:do|दो|2)\s+(?:kamre?|कमरे?|room)/i) ||
      text.match(/(?:ek|एक|1)\s+(?:kamre?|कमरे?|room)/i) ||
      // studio / 1RK
      text.match(/(?:studio|1\s*RK|1rk|ek[\s-]?room[\s-]?kitchen)/i);

    if (bhkM) {
      const raw = bhkM[1] || bhkM[0];
      let n;
      if (/studio|1rk|1\s*rk|ek[\s-]?room[\s-]?kitchen/i.test(raw)) n = "studio";
      else if (/teen|tin|three|3|तीन|थ्री|तिन/.test(raw)) n = "3";
      else if (/do|don|two|2|दो|टू/.test(raw)) n = "2";
      else if (/ek|one|1|एक|वन/.test(raw)) n = "1";
      else if (/char|chaar|four|4|चार|फोर|फ़ोर/.test(raw)) n = "4";
      else n = raw.replace(/\D/g, "") || raw;
      q.bhk = n === "studio" ? "Studio/1RK" : `${n}BHK`;
    }
  }

  // ── Budget ───────────────────────────────────────────────────────────────────
  // Word-number table covers Hindi, Urdu, Marathi spoken forms + Hinglish phonetics
  if (!q.budget) {
    const wordNumMap = {
      // Romanized Hindi/Urdu
      ek: "1", do: "2", "dhai": "2.5", teen: "3", char: "4", chaar: "4",
      paanch: "5", panch: "5", chhe: "6", saat: "7", saath: "7", aath: "8",
      nau: "9", das: "10", gyarah: "11", barah: "12", terah: "13", chaudah: "14",
      pandrah: "15", solah: "16", satrah: "17", atharah: "18", unnis: "19",
      bees: "20", pachees: "25", pachis: "25", tees: "30", paintees: "35",
      chalis: "40", paintaalis: "45", pachaas: "50", pachpan: "55",
      saath: "70", sattar: "70", sitter: "70", assi: "80", nabbe: "90", sau: "100",
      // Marathi romanized
      ek: "1", don: "2", tin: "3", char: "4", paach: "5", sahaa: "6",
      saat: "7", aath: "8", nav: "9", daha: "10", pandhra: "15", vees: "20",
      panchavees: "25", tees: "30", chalees: "40", panna: "50", sattar: "70",
      // Devanagari
      एक: "1", दो: "2", "डेढ़": "1.5", ढाई: "2.5", तीन: "3", चार: "4",
      पाँच: "5", पांच: "5", छह: "6", सात: "7", आठ: "8", नौ: "9",
      दस: "10", ग्यारह: "11", बारह: "12", पंद्रह: "15", बीस: "20",
      पच्चीस: "25", तीस: "30", पैंतीस: "35", चालीस: "40", पैंतालीस: "45",
      पचास: "50", साठ: "60", सत्तर: "70", अस्सी: "80", नब्बे: "90", सौ: "100",
      // Marathi Devanagari
      दोन: "2", तीन: "3", पाच: "5", सहा: "6", सात: "7", आठ: "8", नऊ: "9",
      दहा: "10", पंधरा: "15", वीस: "20", पंचवीस: "25", तीस: "30",
      पन्नास: "50", सत्तर: "70",
    };
    let normText = text;
    for (const [word, digit] of Object.entries(wordNumMap)) {
      normText = normText.replace(new RegExp(`\\b${word}\\b`, "gi"), digit);
    }
    // Range pattern: "50 se 70 lakh" → pick upper bound
    const rangeM = normText.match(/(\d+(?:\.\d+)?)\s*(?:se|to|-)\s*(\d+(?:\.\d+)?)\s*(?:lakh|lac|लाख|लख|lacs)/i)
                || normText.match(/(\d+(?:\.\d+)?)\s*(?:se|to|-)\s*(\d+(?:\.\d+)?)\s*(?:crore|cr\.?\b|करोड़|कोटी|karor|karore|karod)/i);
    if (rangeM) {
      const unit = /crore|cr\b|करोड़|कोटी|karor|karore|karod/i.test(normText) ? "crore" : "lakh";
      q.budget = `${rangeM[1]}-${rangeM[2]} ${unit}`;
    } else {
      const croreM = normText.match(/(\d+(?:\.\d+)?)\s*(?:crore|cr\.?\b|करोड़|कोटी|karor|karore|karod|koti|कोटी)/i);
      const lakhM  = normText.match(/(\d+(?:\.\d+)?)\s*(?:lakh|lac|लाख|लख|lacs|laakh)/i);
      if (croreM) q.budget = `${croreM[1]} crore`;
      else if (lakhM) q.budget = `${lakhM[1]} lakh`;
    }
  }

  // ── Timeline ─────────────────────────────────────────────────────────────────
  if (!q.timeline) {
    if (/\b(?:immediately|abhi[\s-]?chahiye|turant|jaldi[\s-]?chahiye|urgently|asap|as[\s-]?soon|kal[\s-]?chahiye|ready[\s-]?possession|ready[\s-]?to[\s-]?move|6[\s-]?month|6[\s-]?mahine|this[\s-]?year|is[\s-]?saal|is[\s-]?mahine|aaj|kal|next[\s-]?month|agla[\s-]?mahina|possession[\s-]?chahiye|abhi[\s-]?lena|shift[\s-]?karna|tayaar[\s-]?hoon)\b/i.test(t))
      q.timeline = "immediate";
    else if (/\b(?:next[\s-]?year|agle[\s-]?saal|pudh(?:il|cha)[\s-]?varsh|1[\s-]?(?:year|sal|saal|वर्ष|साल)|2026|ek[\s-]?saal[\s-]?mein|within[\s-]?a[\s-]?year)\b/i.test(t))
      q.timeline = "next year";
    else if (/\b(?:2[\s-]?(?:se|to|-)?[\s-]?3[\s-]?(?:year|saal|sal|वर्ष)|2-3|baad[\s-]?mein|later|phir[\s-]?dekhenge|baad[\s-]?mein[\s-]?dekhte|jab[\s-]?milega|abhi[\s-]?nahi|sochna[\s-]?hai|time[\s-]?lagega|2[\s-]?saal|teen[\s-]?saal|3[\s-]?year)\b/i.test(t))
      q.timeline = "2-3 years";
  }
}

function buildSystemPrompt(lead, knowledgeContext, language, agentConfig = {}, qualification = {}) {
  const hasKB = knowledgeContext && knowledgeContext.trim().length > 30;
  const kbBlock = hasKB
    ? `PROJECT KNOWLEDGE BASE — Answer ALL questions directly from this. Never say "I will check" or "let me verify":\n${knowledgeContext}`
    : `PROJECT: ${lead.project || "our project"}`;

  // ── PRIORITY: use system prompt authored in the Agents tab ───────────────
  // The frontend generates the full prompt with {{placeholders}}; we fill them here.
  if (agentConfig.systemPrompt && agentConfig.systemPrompt.trim().length > 50) {
    return agentConfig.systemPrompt
      .replace(/\{\{KNOWLEDGE_BASE\}\}/g,  kbBlock)
      .replace(/\{\{LEAD_NAME\}\}/g,       lead.name         || "ji")
      .replace(/\{\{PROJECT_NAME\}\}/g,    lead.project      || "the project")
      .replace(/\{\{LEAD_BUDGET\}\}/g,     lead.budget       || "not discussed yet");
  }

  // ── FALLBACK: auto-generate (used when no agent is configured in dashboard) ──
  const lang = normalizeLanguageToISO(language || lead.language_preference || lead.language || "auto");
  const langNames = { hi: "Hindi", mr: "Marathi", ta: "Tamil", te: "Telugu", pa: "Punjabi", bn: "Bengali", gu: "Gujarati", kn: "Kannada", ml: "Malayalam", en: "English" };
  const langLabel = langNames[lang];

  // ── Agent config with defaults ────────────────────────────────────────────
  const agentName      = agentConfig.agentName      || "Priya";
  const companyName    = agentConfig.companyName     || "Prophunt";
  const wordCap        = parseInt(agentConfig.wordCap || "30", 10);
  const pitchTone      = agentConfig.pitchTone      || "balanced";       // aggressive | balanced | consultative
  const langStrictness = agentConfig.langStrictness  || "auto";          // auto | hinglish | pure-hindi
  const escalationLine = agentConfig.escalationLine  ||
    "Iske liye main aapko hamare sales expert se connect karti hoon jo bilkul sahi detail de sakenge.";

  // ── Language instruction — fully adaptive, no language barrier ───────────
  // ElevenLabs TTS speaks any language the LLM writes — no need to force Hindi.
  const languageInstruction = `LANGUAGE RULE — STRICT:

SUPPORTED LANGUAGES: Hindi, Marathi, English, Hinglish (mixed Hindi-English).
These are the only languages you speak on this call.

CURRENT CONVERSATION LANGUAGE: ${language}
You MUST reply in this language unless the user EXPLICITLY requests a change (see below).

LANGUAGE MATCHING:
- language=hi or hin → reply PURE Hindi (Devanagari only). No Marathi, no English mixing.
- language=mr → reply PURE Marathi. No Hindi, no English mixing.
- language=en → reply in English only.
- language=hinglish → reply in natural Hinglish. Match their mix ratio.
- Garbled, unclear, or noisy text → stay in current language. Never switch.

LANGUAGE SWITCHING — EXTREMELY STRICT RULE:
NEVER switch language based on what words or script the user uses in their message.
ONLY switch if the user EXPLICITLY asks to change language. Recognized Marathi requests:
  • "marathi mein bolo" / "marathi me baat karo" / "marathi mein baat karte hai"
  • "क्या हम मराटी में बात कर सकते हैं?" / "क्या हम मराठी में बात कर सकते हैं?"
  • "marathi madhye bola" / "marathi bol" / any clear request to speak Marathi
Recognized Hindi requests: "hindi mein bolo" / "hindi me baat karo"
Recognized English requests: "english mein bolo" / "speak in english"
If you see Marathi-looking words, Marathi script, or mixed text but NO explicit request — DO NOT switch. Stay in current language.
Noise, garbled audio, partial words = NOT a language switch request.

LANGUAGE LOCK — CRITICAL:
Once a language is established, MAINTAIN it for the entire conversation.
- If user said "Marathi mein bolo" → stay in Marathi even if they use Hindi words like "हाँ", "ठीक है", "ओके".
- One Hindi/Marathi/English word mixed in = NOT a language switch. It's just natural bilingual speech.

Keep responses SHORT — max ${wordCap} words — one clear point per reply.`;


  // ── Sales approach by tone — written like coaching notes for a human salesperson ──
  const pitchBlock = {
    aggressive: `YOUR STYLE — WARM BUT DRIVEN:
You genuinely want to help them, but you also know a good deal when you see one — and you want them to not miss it.
- Always answer their question first, THEN naturally move toward next step.
- After sharing project info, casually invite: "Ek baar aake dekh lo — 20-30 minute mein picture clear ho jaati hai. Kab free ho?"
- If they say "sochna hai": "Haan bilkul, sochna chahiye itne bade decision mein. Bas ek site visit karo na — bina dekhe sochna thoda mushkil hota hai. Weekend chalega?"
- Second soft refusal: let it go warmly. "Koi baat nahi. Main WhatsApp pe details bhej deti hoon — jab mann kare dekh lena."
- Budget objection: "Dekhiye, EMI mein tod dein toh figure itna bada nahi lagta. Exact number bata doon?"
- Never pressure. Urgency should feel real, not manufactured.`,

    balanced: `YOUR STYLE — HELPFUL FRIEND WHO KNOWS REAL ESTATE:
Think of yourself as that friend who happens to know everything about Pune property market. You're genuinely curious about what they need, and you want to find them the right fit — not just close a sale.
- Listen first, sell second. Let them talk.
- Qualify naturally through conversation — purpose, area, budget, BHK, timeline. ONE at a time, woven into chat.
- Once budget is clear: "Main aapke liye kuch options shortlist karti hoon — WhatsApp number de sakte ho jahan details bhejoon?"
- Build value with KB facts before inviting site visit.
- Offer choice, never pressure: "Main WhatsApp pe bhi bhej sakti hoon, ya ek baar personally site dekh sako — jo comfortable ho."
- Close warmly: "Bahut achha laga baat karke! Details bhejti hoon. Apna khayaal rakhna. Namaste!"`,

    consultative: `YOUR STYLE — TRUSTED ADVISOR, ZERO PRESSURE:
You are not selling. You are helping someone make a good decision — possibly the biggest financial decision of their life. That's a responsibility, not a sales opportunity.
- Understand their situation completely before suggesting anything.
- Be honest: if something doesn't fit their needs, say so. They'll trust you more for it.
- Never mention site visit if they seem uninterested. Let them come to it.
- If they're not ready: "Koi baat nahi, koi pressure nahi. Jab bhi ready ho — main hoon. Number pe call karo ya WhatsApp karo."
- Your goal: leave them feeling heard, respected, and well-informed. A good experience today = a referral tomorrow.`,
  }[pitchTone] || `YOUR STYLE — HELPFUL FRIEND WHO KNOWS REAL ESTATE:
Think of yourself as that friend who happens to know everything about Pune property market. You're genuinely curious about what they need, and you want to find them the right fit — not just close a sale.
- Listen first, sell second. Let them talk.
- Qualify naturally through conversation — purpose, area, budget, BHK, timeline. ONE at a time, woven into chat.
- Once budget is clear: "Main aapke liye kuch options shortlist karti hoon — WhatsApp number de sakte ho jahan details bhejoon?"
- Build value with KB facts before inviting site visit.
- Offer choice, never pressure: "Main WhatsApp pe bhi bhej sakti hoon, ya ek baar personally site dekh sako — jo comfortable ho."
- Close warmly: "Bahut achha laga baat karke! Details bhejti hoon. Apna khayaal rakhna. Namaste!"`;

  // ── Qualification memory — injected so LLM never re-asks what's already known ──
  const knownFacts = [];
  if (qualification.purpose) knownFacts.push(`• Purpose: "${qualification.purpose}" ← ALREADY CONFIRMED, do NOT ask again`);
  if (qualification.budget)  knownFacts.push(`• Budget: "${qualification.budget}" ← ALREADY CONFIRMED, do NOT ask again`);
  if (qualification.bhk)     knownFacts.push(`• BHK: "${qualification.bhk}" ← ALREADY CONFIRMED, do NOT ask again`);
  if (qualification.timeline) knownFacts.push(`• Timeline: "${qualification.timeline}" ← ALREADY CONFIRMED, do NOT ask again`);
  const memoryBlock = knownFacts.length > 0
    ? `\n━━━ WHAT YOU ALREADY KNOW — DO NOT RE-ASK ━━━\n${knownFacts.join("\n")}\nUse these facts naturally. Reference them. Never re-ask.\n`
    : "";

  // ── Marathi mode — full override when language is explicitly locked to Marathi ──
  const marathiLockBlock = (language === "mr")
    ? `
⚠️ LANGUAGE LOCKED TO MARATHI — DEVANAGARI SCRIPT ONLY ⚠️

The user asked to speak in Marathi. Every word you say MUST be in Marathi written in Devanagari script.

SCRIPT RULE — NON-NEGOTIABLE:
✓ CORRECT: "नमस्कार, मी ${agentName} बोलतेय ${companyName} मधून. कसे आहात?"
✗ WRONG: "Namaskar, mi ${agentName} boltey ${companyName} madhun. Kase aahat?"
✗ WRONG: "हाँ बिल्कुल" (Hindi) — use Marathi: "हो, नक्कीच"

English proper nouns (${companyName}, Mahindra Citadel, RERA, WhatsApp) may stay in English.
Numbers should be in digits — say them in Marathi words (e.g. "पन्नास लाख" or "50 लाख").
ALL other words must be pure Devanagari Marathi.

MARATHI EXAMPLE PHRASES — USE THESE AS YOUR STYLE GUIDE:
• Check-in: "हो, बोला — मी ऐकतेय."
• Agreement: "हो, नक्की." / "बरोबर आहे." / "अगदी बरोबर."
• Warm opener: "अरे वा, गुंतवणुकीसाठी बघतोय? अगदी योग्य वेळ आहे."
• After info shared: "एकदा site ला या — 20-30 मिनिटांत सगळं clear होतं. कधी वेळ आहे?"
• Budget pushback: "EMI मध्ये मोडलं तर इतकं मोठं वाटत नाही. Exact amount सांगू का?"
• Soft close: "Details WhatsApp वर पाठवते — WhatsApp number द्याल का?"
• Goodbye: "बोलून खूप बरं वाटलं! काळजी घ्या. नमस्कार!"
• Unclear audio: "माफ करा, नीट ऐकू आलं नाही. एकदा परत सांगाल का?"
• Self-correct: "हो हो, बरोबर पकडलंत — चुकलो मी."
• Unknown info: "नक्की माहीत नाही — confirm करून सांगतो/सांगते."

NEVER output Hindi words like "बिल्कुल", "हाँ", "ठीक है", "नहीं", "अच्छा".
Use Marathi: "नक्कीच", "हो", "ठीक आहे", "नाही", "बरं".
`
    : "";

  return `You are ${agentName}, a real estate consultant calling from ${companyName}. You've been doing this for years and you genuinely enjoy it — helping people find the right home or investment is something you take seriously.

Think of yourself as that one friend who happens to know everything about the Pune/Mumbai property market. Not a salesperson. Not a bot. A real person who picks up the phone, listens carefully, and talks like a human being.

${kbBlock}

ABOUT THIS CALL:
- Lead name: ${lead.name}
- Interested in: ${lead.project || "property in general"}
- Budget: ${lead.budget || "not discussed yet"}
${memoryBlock}${marathiLockBlock}

${languageInstruction}

━━━ HOW YOU TALK — READ THIS CAREFULLY ━━━

YOU ARE NOT A SCRIPT-READER. You are a person having a real conversation.

1. REACT GENUINELY — When someone says something, react like a human would.
   "Arrey wah, investment ke liye dekh rahe ho? Bahut sahi time hai abhi." — not a robotic "Noted."

2. FOLLOW THE CUSTOMER — If they jump to hospitals, go there. If they ask about a competitor, answer it fully and honestly. Qualification questions will come naturally — don't force them back to the funnel.

3. ONE QUESTION AT A TIME — Never ask two things at once. Always answer their question first, THEN ask yours.

4. KEEP IT SHORT — Max ${wordCap} words per response. On a phone call, long speeches make people zone out.

5. ADMIT UNCERTAINTY LIKE A HUMAN — You don't know everything, and that's fine.
   "Jahan tak mujhe pata hai, possession 2027 ke aaspaas hai — main confirm karke bata sakti hoon."
   Never make up exact numbers for projects not in the KB.

6. SELF-CORRECT WITHOUT DRAMA — If they correct you, just say "Haan haan, sahi pakda aapne" and move on. No long apologies, no over-explaining.

7. BE WARM, NOT PERFORMATIVE — "Achha achha", "Arrey wah", "Sahi baat hai" feel natural when used genuinely. But don't layer three reactions together. One real reaction per turn.

8. A LITTLE HUMOR GOES A LONG WAY — A light remark makes you memorable.
   "Dus lakh mein Pune West mein... haan, thoda mushkil hai — but chalein, dekhte hain kya nikalta hai!" (warm, not dismissive)

9. MOST RESPONSES START DIRECTLY WITH THE ANSWER — no filler word, no "Haan/Achha/Hmm" preamble. Just answer.
   A real person doesn't open every sentence with a verbal tic — only a bot does that.

10. FILLERS ARE RARE, NOT A HABIT — use a short reaction like "Arrey wah" / "Achha" / "Samjha" / "Dekhiye"
    at most once every 4-5 turns, ONLY when something genuinely surprising or noteworthy was just said
    (e.g. they revealed a big budget, or a real objection). NEVER use one two turns in a row.
    NEVER start with "Haan" or "Accha" as a default habit — vary it, or skip it entirely most of the time.

━━━ WHAT YOU'RE TRYING TO FIND OUT — weave naturally into conversation ━━━
1. Purpose — investment ya khud rehne ke liye?
2. Area — already know: ${lead.project ? `${lead.project} area` : "not discussed"}
3. Budget — ${lead.budget || "not discussed yet"}
4. BHK preference
5. Timeline — kitne time mein decision?
6. WhatsApp number — after budget confirmed: "Main details bhejti hoon — WhatsApp number share karoge?"
7. Site visit or WhatsApp details — offer choice, never push

${pitchBlock}

━━━ HANDLING DIFFERENT TYPES OF QUESTIONS ━━━

OUR PROJECT (KB data available):
→ Use exact KB facts — price, RERA, amenities, possession. Never guess these.
→ Only mention BHK types that exist in the KB. Never invent configurations.
→ Something unclear? Clarify first: "Aap loan ki baat kar rahe hain ya configuration ki?"

SAME DEVELOPER, DIFFERENT PROJECT (Mahindra Vivante, Happinest, Eden, etc.):
→ Answer from your general knowledge about that project.
→ Then naturally: "Citadel Pimpri mein bhi dekha? Usi range mein bahut value milti hai wahan."

COMPETITOR PROJECT (Shapoorji, Godrej, Lodha, Kolte Patil, etc.):
→ Be genuinely helpful. Say something real and positive about the developer — they're asking because they're doing research, help them.
   "Shapoorji Pallonji — bahut bada aur bharosemand group hai yaar, quality ke liye jaane jaate hain. Treetopia Jadhavwadi mein kafi popular project hai unka. Pricing ke baare mein jaanna tha, ya configuration?"
→ Fully answer what they asked. Then — only once, lightly — mention our project if it's a natural fit.
→ Never put down competitors. It makes you look small.

COMPLETELY UNKNOWN / OBSCURE PROJECT (small local builder, project you've genuinely never heard of):
→ A web search has been run and results injected above as "WEB SEARCH RESULTS" — USE THEM to answer accurately.
→ If web search results are available: answer confidently using those facts, then naturally pivot to our project.
→ If no web search results are present: be honest and warm — "Yaar, is specific project ke baare mein meri detailed info nahi hai abhi. Developer ka naam ya location confirm karo, main pata karta/karti hoon."
→ NEVER make up prices, possession dates, or RERA numbers you don't have.
→ After answering (with or without web data): pivot once — "Waise, aap ki requirement ke hisaab se hamare paas bhi ek strong option hai — compare karein?"

LOCATION / AMENITY QUESTIONS (nearest metro, hospital, school, distance, connectivity):
→ Check "WEB SEARCH RESULTS" injected above — a live search was run with the project name + city, so results are geographically accurate for this specific location.
→ Use those results to answer precisely: station names, distances, travel times.
→ If no search results available: use general knowledge but qualify it — "generally", "approximately" — never state a wrong distance as a fact.
→ These questions build huge trust — answer them well and specifically.
   Good: "Pimpri metro station sabse paas hai — roughly 1.5 km, 5 minute ki drive. DY Patil hospital bhi 10 minute mein milta hai."
   Bad: "Hamare paas yeh information nahi hai." ← never say this

━━━ NEVER DO THESE ━━━
✗ "Mujhe pata nahi" — rephrase: "Main confirm karke bata sakti hoon"
✗ Repeat the project name twice in the same sentence — sounds like an ad
✗ Ask your next question before fully answering theirs
✗ Ignore what they said and jump to your funnel step
✗ Start every response with "Bilkul!" — it becomes a tell
✗ Make up prices, RERA numbers, or possession dates you don't have in KB
✗ Give a 5-sentence answer when 2 sentences will do

━━━ MID-CALL CHECK-INS ━━━
If the user says ONLY "hello", "hi", "हेलो", "हाँ", "haan", "are you there" at any point during the conversation — DO NOT re-introduce yourself. Just say "हाँ, बोलिए" or "हाँ, हूँ यहाँ" and wait. NEVER restart the call or repeat your name.
This call is ALREADY in progress — treat it as ongoing no matter what.

━━━ CLOSING ━━━
Natural end: "Bahut achha laga baat karke ${lead.name} ji! Details bhejti hoon WhatsApp pe. Apna khayaal rakhna. Namaste!"
Not interested: "Koi baat nahi, bilkul. Kabhi bhi sawaal ho — main hoon. Take care!"
`;
}

async function timed(service, fn) {
  const start = Date.now();
  try {
    return await fn();
  } finally {
    serviceLatency.labels(service).observe(Date.now() - start);
  }
}

async function callCrmAdapter(method, url, payload) {
  const response = await timed("crm_adapter", () =>
    axios({
      method,
      url: `${config.services.crmAdapter}${url}`,
      data: payload,
      timeout: 10000,
    })
  );
  return response.data;
}

async function fetchLeadByPhone(phone) {
  return callCrmAdapter("get", `/api/leads/by-phone/${encodeURIComponent(phone)}`);
}

async function fetchDialableLeads(campaignId, limit, filters = {}) {
  const data = await callCrmAdapter("post", "/api/leads/fetch-dialable", {
    campaign_id: campaignId,
    limit,
    filters,
  });
  return data.leads || [];
}

async function pushToCRM(leadId, outcome) {
  return callCrmAdapter("patch", `/api/leads/${leadId}/update`, { outcome });
}

async function persistCallLog(session, outcome, durationSec, finalStatus) {
  const tenantId =
    session.campaign?.tenant_id ||
    session.campaign?.tenantId ||
    session.lead?.tenant_id ||
    session.lead?.tenantId ||
    process.env.DEFAULT_TENANT_ID ||
    "";
  if (!tenantId) {
    console.warn("[call-log] skipped platform persistence because tenant_id was not available", {
      call_sid: session.callSid,
      lead_id: session.lead?.id,
    });
    return null;
  }
  const payload = {
    tenant_id: tenantId,
    campaign_id: session.campaign?.id || session.campaign?.campaign_id || null,
    lead_id: session.lead?.id || null,
    phone: session.lead?.phone || "unknown",
    status: finalStatus,
    call_metadata: {
      provider: session.telephony?.provider || "simulated",
      call_id: session.telephony?.voiceId || session.telephony?.callSid || session.callSid,
      duration_sec: durationSec,
      started_at: session.startedAt,
      ended_at: session.endedAt,
      outcome,
      transcript_summary: outcome.transcript_summary,
      full_transcript: outcome.full_transcript,
      recording_url: outcome.recording_url,
      recordings: session.recordings || {},
      lead_name: session.lead?.name || null,
    },
  };
  try {
    const response = await axios.post(`${config.services.platformApi}/internal/calls`, payload, {
      headers: {
        "Content-Type": "application/json",
        "X-Internal-Token": config.internalToken,
      },
      timeout: 10000,
    });
    return response.data;
  } catch (error) {
    console.warn("[call-log] platform persistence failed", {
      call_sid: session.callSid,
      error: error.response?.data || error.message,
    });
    return null;
  }
}

async function getKnowledgeContext(projectId, transcript) {
  if (!projectId || !transcript) {
    return "";
  }
  try {
    const response = await timed("knowledge_service", () =>
      axios.get(`${config.services.knowledge}/projects/${projectId}/query`, {
        params: { q: transcript },
        timeout: 10000,
      })
    );
    const matches = response.data.matches || [];
    return matches.map((match) => `[${match.section}] ${match.text}`).join("\n");
  } catch {
    return "";
  }
}

// ── In-process VAD — RMS energy + zero-crossing rate (~0.05ms vs ~15ms HTTP) ──
// Eliminates one HTTP round-trip per 20ms audio frame. Tune VAD_THRESHOLD env var.
const VAD_RMS_THRESHOLD     = parseInt(process.env.VAD_THRESHOLD      || "420", 10);
const VAD_ZCR_THRESHOLD     = parseFloat(process.env.VAD_ZCR_THRESHOLD || "0.08");

function detectSpeech(pcm16Buffer) {
  if (!pcm16Buffer || pcm16Buffer.length < 4) return false;
  const samples = Math.floor(pcm16Buffer.length / 2);
  let sumSq = 0, zeroCrossings = 0;
  let prev = 0;
  for (let i = 0; i < pcm16Buffer.length - 1; i += 2) {
    const s = pcm16Buffer.readInt16LE(i);
    sumSq += s * s;
    if ((s >= 0) !== (prev >= 0)) zeroCrossings++;
    prev = s;
  }
  const rms = Math.sqrt(sumSq / samples);
  const zcr = zeroCrossings / samples;
  // Speech has both energy (rms) AND frequency content (zcr).
  // Pure silence has low rms. Background noise has low zcr.
  return rms > VAD_RMS_THRESHOLD && zcr > VAD_ZCR_THRESHOLD;
}

async function detectLanguage(audioBuffer) {
  const form = new FormData();
  form.append("audio", ensureWavBuffer(audioBuffer), { filename: "sample.wav", contentType: "audio/wav" });
  const response = await timed("stt", () =>
    axios.post(`${config.services.stt}/detect-language`, form, {
      headers: form.getHeaders(),
      timeout: Math.min(config.sttTimeoutMs, 15000),
    })
  );
  return response.data;
}

// ── Real estate web search via Tavily ────────────────────────────────────────
// Fires when user asks something not covered by the KB — project info OR
// location/amenity questions (metro, hospital, school, connectivity etc.)
// Builds a geo-aware query using the project name + city for accurate results.
// Fails silently so calls are never interrupted.

const PROJECT_KEYWORDS = /\b(project|residency|residences|towers|heights|society|township|enclave|gardens|meadows|villas|estate|avenue|park|city|phase|sector|vivanta|vivante|happinest|treetopia|lodha|godrej|shapoorji|prestige|brigade|sobha|kolte|piramal|tata|birla|mahindra(?!\s+citadel)|runwal|raymond|rustomjee|hiranandani|oberoi|wadhwa|kalpataru|nirmal|raheja|kanakia|sun\s+builders|sumadhura|assetz|puravankara|salarpuria|adarsh|mantri|mpa|pride|marvel|majestique|panorama|saarrthi|naiknavare|rohan|nyati|kohinoor|kunal|gagan|vascon|panchshil|abil|k\s+raheja|el\s+cid|mohamadiya)\b/i;

const LOCATION_KEYWORDS = /\b(metro|station|hospital|school|college|university|mall|market|airport|highway|expressway|flyover|bus\s*stop|railway|train|connectivity|distance|nearest|nearby|close\s*to|how\s*far|minutes?\s*from|km\s*from|road|route|reach|travelling?|commute|traffic|parking|garden|park|lake|temple|church|mosque|police|fire\s*station|post\s*office|bank|atm|petrol|pharmacy|clinic|doctor)\b/i;

async function searchWebContext(userText, lead, knowledgeContext) {
  const tavilyKey = process.env.TAVILY_API_KEY;
  if (!tavilyKey) return null;

  const text = userText.toLowerCase();
  const isProjectQuery  = PROJECT_KEYWORDS.test(userText);
  const isLocationQuery = LOCATION_KEYWORDS.test(userText);

  if (!isProjectQuery && !isLocationQuery) return null;

  // For location questions about our own project — check KB first, skip search if answered
  const projectName = lead?.project || "";
  const projectCity = lead?.city || "Pune";
  const ourProjectWords = projectName.toLowerCase().split(/\s+/).filter(w => w.length > 3);
  const isAboutOurProject = ourProjectWords.length > 0 && ourProjectWords.some(w => text.includes(w));

  if (isAboutOurProject && knowledgeContext && knowledgeContext.length > 100) {
    // KB exists for our project — only search if it's a location question KB likely won't have
    if (!isLocationQuery) return null;
    // Location question about our project and KB doesn't mention the specific topic
    const locationTopic = (userText.match(LOCATION_KEYWORDS) || [])[0] || "";
    if (locationTopic && knowledgeContext.toLowerCase().includes(locationTopic.toLowerCase())) return null;
  }

  // Build a precise, geo-aware query
  let query;
  if (isLocationQuery && projectName) {
    // e.g. "nearest metro station to Mahindra Citadel Pimpri Pune"
    query = `${userText} near ${projectName} ${projectCity} India`;
  } else if (isProjectQuery) {
    // e.g. "Shapoorji Treetopia Jadhavwadi Pune price possession configuration 2025"
    query = `${userText} ${projectCity} India real estate price possession 2025`;
  } else {
    query = `${userText} ${projectName} ${projectCity} India`;
  }

  const t0 = Date.now();
  try {
    const res = await axios.post(
      "https://api.tavily.com/search",
      {
        api_key: tavilyKey,
        query,
        search_depth: "basic",
        max_results: 3,
        include_answer: true,
        include_raw_content: false,
      },
      { timeout: 4000 }
    );
    const answer = (res.data?.answer || "").trim();
    const snippets = (res.data?.results || [])
      .slice(0, 2)
      .map(r => r.content?.slice(0, 400))
      .filter(Boolean)
      .join("\n");
    const context = (answer || snippets).slice(0, 900);
    if (!context) return null;
    console.log(`[search] Tavily ${Date.now()-t0}ms type=${isLocationQuery?"location":"project"} q="${query.slice(0,70)}"`);
    return context;
  } catch (err) {
    console.warn(`[search] Tavily failed (${Date.now()-t0}ms): ${err.message}`);
    return null;
  }
}

async function transcribeAudio(audioBuffer, language = "auto") {
  const form = new FormData();
  form.append("audio", ensureWavBuffer(audioBuffer), { filename: "audio.wav", contentType: "audio/wav" });
  form.append("language", language);
  const response = await timed("stt", () =>
    axios.post(`${config.services.stt}/transcribe`, form, {
      headers: form.getHeaders(),
      timeout: config.sttTimeoutMs,
    })
  );
  return response.data;
}

// ── Direct Sarvam STT — bypasses internal STT microservice, saves one hop ────
// Sarvam accepts: POST /speech-to-text  multipart { file, model, language_code }
// Response: { transcript, language_code, ... }
const SARVAM_LANG_MAP = {
  // 2-letter ISO codes
  "hi": "hi-IN", "en": "en-IN", "mr": "mr-IN",
  "ta": "ta-IN", "te": "te-IN", "kn": "kn-IN",
  "gu": "gu-IN", "bn": "bn-IN", "pa": "pa-IN",
  "ml": "ml-IN", "od": "od-IN",
  // 3-letter ISO codes returned by ElevenLabs Scribe STT
  "hin": "hi-IN", "eng": "en-IN", "mar": "mr-IN",
  "tam": "ta-IN", "tel": "te-IN", "kan": "kn-IN",
  "guj": "gu-IN", "ben": "bn-IN", "pan": "pa-IN",
  "mal": "ml-IN",
};

const GEMINI_LANG_NAMES = {
  hi: "Hindi", en: "English", mr: "Marathi", ta: "Tamil", te: "Telugu",
  kn: "Kannada", gu: "Gujarati", bn: "Bengali", pa: "Punjabi", ml: "Malayalam",
};

async function transcribeAudioGemini(audioBuffer, language = "auto") {
  const wav = ensureWavBuffer(audioBuffer);
  const langHint = GEMINI_LANG_NAMES[language]
    ? ` The speaker is likely speaking ${GEMINI_LANG_NAMES[language]}, possibly mixed with English (Hinglish).`
    : " The speaker may be speaking Hindi, English, Marathi, or a Hindi-English mix (Hinglish).";
  const prompt = `Transcribe EXACTLY what is spoken in this audio clip. Output ONLY the transcript text in its `
    + `original language/script — no translation, no explanation, no quotes, no labels.${langHint} `
    + `If the audio is silent, noise, or unintelligible, output nothing.`;
  const t0 = Date.now();
  const response = await timed("stt_gemini", () =>
    axios.post(
      `https://generativelanguage.googleapis.com/v1beta/models/${process.env.GEMINI_STT_MODEL || "gemini-2.5-flash"}:generateContent`,
      {
        contents: [{
          parts: [
            { text: prompt },
            { inline_data: { mime_type: "audio/wav", data: wav.toString("base64") } },
          ],
        }],
        generationConfig: { temperature: 0, maxOutputTokens: 200 },
      },
      {
        headers: { "x-goog-api-key": process.env.GEMINI_API_KEY, "Content-Type": "application/json" },
        timeout: 12000,
      }
    )
  );
  const text = (response.data?.candidates?.[0]?.content?.parts?.[0]?.text || "").trim();
  console.log(`[stt-gemini] latency=${Date.now()-t0}ms text="${text.slice(0, 80)}"`);
  return { text, language: language === "auto" ? "hi" : language };
}

async function transcribeAudioDirect(audioBuffer, language = "auto", providerOverride = null) {
  const elevenKey = process.env.ELEVENLABS_API_KEY;
  const sarvamKey = process.env.SARVAM_API_KEY;

  // STT_PROVIDER controls which engine runs first.
  // "elevenlabs" → ElevenLabs Scribe first (better accuracy, auto language detection)
  // Default      → sarvam
  // providerOverride (per-session, from browser test harness) takes precedence over env.
  const sttProvider = (providerOverride || process.env.STT_PROVIDER || "sarvam").toLowerCase();
  const useSarvamFirst = sttProvider === "sarvam" && !!sarvamKey;

  // ── Gemini (multimodal audio-in transcription) — only when explicitly selected
  // via provider override (browser test lab). Gemini has no dedicated streaming
  // ASR endpoint like Sarvam/Deepgram; this sends the audio inline to generateContent
  // and asks for an exact transcript back. Batch-only, so it won't beat streaming
  // STT on latency, but it's a real bake-off entry for accuracy on Hindi/Hinglish. ──
  if (sttProvider === "gemini" && process.env.GEMINI_API_KEY) {
    const geminiResult = await transcribeAudioGemini(audioBuffer, language).catch((err) => {
      console.warn("[stt-gemini] failed, falling back:", err.message);
      return null;
    });
    if (geminiResult) return geminiResult;
    // fall through to Sarvam/ElevenLabs below on failure
  }

  // ── Sarvam Saarika v2.5 (primary when STT_PROVIDER=sarvam) ───────────────
  if (useSarvamFirst) {
    const wav = ensureWavBuffer(audioBuffer);
    const form = new FormData();
    form.append("file", wav, { filename: "audio.wav", contentType: "audio/wav" });
    form.append("model", "saarika:v2.5");
    const langCode = SARVAM_LANG_MAP[language] || (language === "auto" ? undefined : language);
    if (langCode) form.append("language_code", langCode);
    try {
      const t0 = Date.now();
      const response = await timed("stt_sarvam", () =>
        axios.post(
          `${process.env.SARVAM_API_URL || "https://api.sarvam.ai"}/speech-to-text`,
          form,
          {
            headers: { ...form.getHeaders(), "api-subscription-key": sarvamKey },
            timeout: 12000,
          }
        )
      );
      const d = response.data;
      const detectedLang = d.language_code?.split("-")[0] || language;
      console.log(`[stt-sarvam] latency=${Date.now()-t0}ms lang=${detectedLang} text="${(d.transcript || "").slice(0, 80)}"`);
      return {
        text:     d.transcript || "",
        language: detectedLang,
      };
    } catch (err) {
      console.warn("[stt-sarvam] failed, falling back to ElevenLabs:", err.message);
      // fall through to ElevenLabs below
    }
  }

  // ── ElevenLabs Scribe STT (primary when STT_PROVIDER=elevenlabs, else fallback) ──
  if (elevenKey) {
    const wav = ensureWavBuffer(audioBuffer);
    const form = new FormData();
    form.append("file", wav, { filename: "audio.wav", contentType: "audio/wav" });
    form.append("model_id", "scribe_v1");
    const ELEVEN_LANG_MAP = {
      "hi": "hi", "en": "en", "mr": "mr", "ta": "ta",
      "te": "te", "kn": "kn", "gu": "gu", "bn": "bn", "pa": "pa",
    };
    const langCode = language === "auto" ? null : (ELEVEN_LANG_MAP[language] || language);
    if (langCode) form.append("language_code", langCode);
    try {
      const t0 = Date.now();
      const response = await timed("stt_elevenlabs", () =>
        axios.post(
          "https://api.elevenlabs.io/v1/speech-to-text",
          form,
          {
            headers: { ...form.getHeaders(), "xi-api-key": elevenKey },
            timeout: 15000,
          }
        )
      );
      const d = response.data;
      const detectedLang = d.language_code?.split("-")[0] || language;
      console.log(`[stt-elevenlabs] latency=${Date.now()-t0}ms lang=${detectedLang} text="${(d.text || "").slice(0, 80)}"`);
      return {
        text:     d.text || "",
        language: detectedLang,
      };
    } catch (err) {
      console.warn("[stt-elevenlabs] failed, falling back to microservice:", err.message);
    }
  }

  // ── Sarvam fallback when STT_PROVIDER=elevenlabs but ElevenLabs failed ────
  if (!useSarvamFirst && sarvamKey) {
    const wav = ensureWavBuffer(audioBuffer);
    const form = new FormData();
    form.append("file", wav, { filename: "audio.wav", contentType: "audio/wav" });
    form.append("model", "saarika:v2.5");
    const langCode = SARVAM_LANG_MAP[language] || (language === "auto" ? undefined : language);
    if (langCode) form.append("language_code", langCode);
    try {
      const t0 = Date.now();
      const response = await timed("stt_sarvam_fb", () =>
        axios.post(
          `${process.env.SARVAM_API_URL || "https://api.sarvam.ai"}/speech-to-text`,
          form,
          {
            headers: { ...form.getHeaders(), "api-subscription-key": sarvamKey },
            timeout: 12000,
          }
        )
      );
      const d = response.data;
      const detectedLang = d.language_code?.split("-")[0] || language;
      console.log(`[stt-sarvam-fb] latency=${Date.now()-t0}ms lang=${detectedLang} text="${(d.transcript || "").slice(0, 80)}"`);
      return {
        text:     d.transcript || "",
        language: detectedLang,
      };
    } catch (err) {
      console.warn("[stt-sarvam-fb] failed, falling back to microservice:", err.message);
    }
  }

  return transcribeAudio(audioBuffer, language);  // last resort: local microservice
}

// Extract a short price snippet from KB context (used to make guided replies KB-aware)
function extractPriceFromKB(knowledgeBase = "") {
  if (!knowledgeBase) return null;
  // Look for price patterns: ₹X Cr, ₹X lakh, X crore, X lacs, etc.
  const priceMatch = knowledgeBase.match(/(?:starting|starts?|from|price|rate|cost)[^\n.]{0,60}(?:₹|rs\.?|inr)\s*[\d,.]+\s*(?:cr(?:ore)?|lakh?|lac|l)/i)
    || knowledgeBase.match(/(?:₹|rs\.?|inr)\s*[\d,.]+\s*(?:cr(?:ore)?|lakh?|lac|l)[^\n.]{0,60}/i)
    || knowledgeBase.match(/(?:2bhk|3bhk|two bhk|three bhk)[^\n.]{0,80}(?:₹|rs\.?|inr)/i);
  return priceMatch ? priceMatch[0].trim() : null;
}

function buildRuleBasedReply(session, userText = "") {
  const text = String(userText || "").toLowerCase();
  const project = session.lead?.project || session.campaign?.project_name || "the project";
  const lang = languageManager.getBaseLanguage(session.callSid);
  const isHindi = lang === "hi";
  const kbPriceSnippet = extractPriceFromKB(session.dynamicVariables?.knowledge_base || "");

  // ── Helpers — MUST be defined before any usage ────────────────────────────
  const T = (en, hi) => isHindi ? hi : en;

  // ── Universal farewell — end call immediately regardless of state ───────────
  // Catches: "thank you", "थैंक यू", "धन्यवाद", "bye", "chalo", etc.
  const universalFarewell = /\b(thank you|thanks|bye|goodbye|alvida|ok bye|ok thanks|chalo ab|ab chalta|achha theek|chalta hoon|chalti hoon|chalte hain)\b|थैंक\s*यू|धन्यवाद|शुक्रिया|अलविदा|बाय\b|चलो\s*अब|ठीक\s*है\s*चलते|चलते\s*हैं/.test(text);
  if (universalFarewell) {
    session.guidedState = "closed";
    return T(
      `Thank you for your time. Have a great day. Goodbye!`,
      `Bahut shukriya aapka waqt dene ke liye. Aapka din shubh ho. Namaste!`
    );
  }

  // ── Intent patterns — Latin (Romanised Hindi) + Devanagari (Sarvam STT output) ──
  const wantsConfiguration = /(?:\b|[^a-z0-9])(?:1|one|ek|2|two|do|3|three|teen|4|four|char)\s*(?:b|v|d)?\s*h\s*k\b|bhk|vhk|dhk|dbhk|vbhk|configuration|config|flat size|carpet|sq ?ft|बीएचके|बी\.?एच\.?के|bhk/.test(text);
  const wantsTwoBhk = /(?:2|two|to|too|do|d)\s*(?:b|v|d)?\s*h\s*k|dbhk|2bhk|two bhk|do bhk|दो\s*(?:बीएचके|बी\s*एच\s*के|bhk)|2\s*(?:बीएचके|bhk)/.test(text);
  const wantsThreeBhk = /(?:3|three|tree|free|teen)\s*(?:b|v|d)?\s*h\s*k|3vhk|3bhk|three bhk|teen bhk|तीन\s*(?:बीएचके|बी\s*एच\s*के|bhk)|3\s*(?:बीएचके|bhk)/.test(text);
  // Positive — Latin Romanised + Devanagari
  const positiveIntent = /yes|yeah|yep|sure|proceed|tell me|go ahead|interested|ok|okay|alright|all right|hello|hi|speaking|here|haan|ji\b|bilkul|theek|sahi|zaroor|batao|bataiye|हाँ|हां|जी|ठीक|बिल्कुल|ज़रूर|जरूर|बताओ|बताइए|बोलिए|सुनिए|सुनें|हा\b/.test(text);
  // Explicit farewell — Latin + Devanagari
  const explicitFarewell = /\b(bye|goodbye|good bye|not interested|no thank|stop calling|remove|alvida|band karo|chhodo|mujhe nahi chahiye)\b|अलविदा|बंद करो|नहीं चाहिए|छोड़ो/.test(text);
  // Negative — Latin + Devanagari
  const negativeIntent = /bye|not interested|stop|later|no\b|not now|busy|nahi\b|nahin\b|na\b|mat\b|baad mein|abhi nahi|नहीं|नही|ना\b|मत\b|बाद में|अभी नहीं|व्यस्त|बिज़ी/.test(text);
  const guidedState = session.guidedState || null;

  if (/price|cost|rate|budget|how much|pricing|daam|kimat|kitna|kitne|paisa|qeemat|रेट|दाम|कीमत|क़ीमत|कितना|कितने|पैसे|रुपए|रुपये|प्राइस|बजट/.test(text)) {
    if (kbPriceSnippet) {
      session.guidedState = "awaiting_site_visit";
      return T(
        `For ${project}: ${kbPriceSnippet}. Premium gated township with clubhouse, pool, gym, and 24/7 security — at a great launch price. Would you like to book a site visit to see it in person?`,
        `${project} mein ${kbPriceSnippet}. Premium gated township hai — clubhouse, pool, gym aur 24/7 security ke saath, abhi launch kimat mein. Kya site visit book karein taki aap personally dekh sakein?`
      );
    }
    session.guidedState = "awaiting_configuration";
    return T(
      `For ${project}, are you looking at 2 BHK or 3 BHK pricing?`,
      `${project} mein do BHK ka rate chahiye ya teen BHK ka?`
    );
  }
  // ── BHK query — give real info first, DON'T jump to callback/site-visit yet ──
  if (wantsTwoBhk || wantsThreeBhk || wantsConfiguration) {
    // If already past BHK info stage, fall through to LLM for follow-up questions
    if (["price_discussed", "awaiting_site_visit", "site_visit_confirmed",
         "awaiting_callback_confirmation", "callback_confirmed"].includes(guidedState)) {
      return null;
    }
    const bhkLabel = isHindi
      ? (wantsThreeBhk ? "teen BHK" : wantsTwoBhk ? "do BHK" : "BHK")
      : (wantsThreeBhk ? "3 BHK" : wantsTwoBhk ? "2 BHK" : "BHK");
    session._bhkType = wantsTwoBhk ? "2" : wantsThreeBhk ? "3" : "any";
    session.guidedState = "bhk_discussed";
    return T(
      `${project} has beautiful ${bhkLabel} apartments in two layouts — Compact and Classic — available in Wings J and K with great views. Ready-to-move units are also available. Want me to share the current pricing?`,
      `${project} mein ${bhkLabel} ke do options hain — Compact aur Classic layout, Wings J aur K mein sundar views ke saath. Ready-to-move units bhi hain. Kya main current kimat bata doon?`
    );
  }

  // ── bhk_discussed → user wants price or follow-up ──────────────────────────
  if (guidedState === "bhk_discussed") {
    const wantsPrice = /price|cost|rate|kitna|kimat|rupaye|budget|lakh|crore|paisa|qeemat|रेट|दाम|कीमत|कितना|कितने|रुपए/.test(text);
    if (wantsPrice || positiveIntent) {
      session.guidedState = "awaiting_site_visit";
      if (kbPriceSnippet) {
        return T(
          `For ${project}: ${kbPriceSnippet}. It's a premium gated community with clubhouse, pool, gym, and 24/7 security — and these are launch prices that will go up soon. Shall I book a site visit so you can see it in person?`,
          `${project} mein ${kbPriceSnippet}. Yeh ek premium gated community hai — clubhouse, pool, gym aur 24/7 security ke saath. Abhi launch price mein mil raha hai, baad mein daam badhenge. Kya main ek site visit arrange karoon taki aap personally dekh sakein?`
        );
      }
      // No KB price snippet — let LLM answer the price, but track state
      session.guidedState = "awaiting_site_visit";
      return null; // LLM will answer with KB price, then we're in awaiting_site_visit
    }
    // Any other question in bhk_discussed — LLM handles with KB
    return null;
  }

  // ── awaiting_site_visit → respond to yes/no on site visit ─────────────────
  if (guidedState === "awaiting_site_visit") {
    // If the user is asking a question or requesting information → let LLM answer
    // Don't treat informational requests like "bataiye/बताइए/kya hai/hospital/college" as "yes"
    const isInfoRequest = /\b(bataiye|batao|bata|kya|kaise|kaisa|kaisi|kitna|kitne|kimat|price|cost|hospital|college|school|mall|metro|location|jagah|amenity|amenities|pool|gym|parking|floor|possession|ready|handover|loan|emi|bank|discount|offer)\b|बताइए|बताओ|क्या|कैसा|कितना|कितने|कीमत|हॉस्पिटल|कॉलेज|स्कूल|मेट्रो|लोकेशन|पार्किंग|पज़ेशन|लोन|ईएमआई|डिस्काउंट/i.test(text);
    if (isInfoRequest) {
      // User is asking a real question — don't push site visit, let LLM answer from KB
      return null;
    }
    // Explicit yes/confirmation — require ≥3 words OR a single known affirmative word.
    // Short/garbage STT (e.g. "ठीक है मोजर") must not auto-confirm a site visit.
    // The regex tests the START of the trimmed text for a clear affirmative.
    const wordCountSV = text.trim().split(/\s+/).filter(w => w.length > 0).length;
    const explicitYes = /^(haan|ha\b|yes|ji\b|bilkul|theek|acha|accha|zaroor|sure|ok\b|okay|chalo|kar do|book karo|book kar|karo|kijiye|lelo|le lo|confirm|done)\b|^हाँ|^हां|^जी\b|^बिल्कुल|^ठीक|^अच्छा|^ज़रूर|^जरूर/i.test(text.trim());
    // Guard: don't confirm from very short fragments (< 2 words) — likely echo/noise
    const cleanConfirm = explicitYes && wordCountSV >= 2;
    if (cleanConfirm) {
      // Move to contact confirmation step before closing — mirrors PDF recommended flow.
      // State: awaiting_contact_confirmation — guided will close after number is given.
      session.guidedState = "awaiting_contact_confirmation";
      return T(
        `Wonderful! I have noted your site visit request for ${project}. Just to confirm — is this the best number for our team to call you back on?`,
        `Bahut achha! ${project} ke liye site visit note kar li hai. Ek baar confirm karein — kya yahi number best hai jis par hamari team aapko call kare?`
      );
    }
    if (negativeIntent) {
      session.guidedState = "price_discussed";
      return T(
        `No problem at all. I can send you the full details, floor plans, and pricing on WhatsApp — would that work?`,
        `Koi baat nahi. Main aapko WhatsApp par full details, floor plan aur pricing bhej sakti hoon — kya theek rahega?`
      );
    }
    // Anything else — let LLM continue the conversation naturally with KB context
    return null;
  }

  // ── price_discussed → continue conversation or offer site visit ────────────
  if (guidedState === "price_discussed") {
    if (positiveIntent) {
      session.guidedState = "awaiting_site_visit";
      return T(
        `I can arrange a site visit at ${project} for you. Our team will confirm the timing. Shall I book it?`,
        `Main ${project} ke liye site visit arrange kar sakti hoon. Hamari team timing confirm kar legi. Karoon book?`
      );
    }
    // Let LLM continue if they have more questions
    return null;
  }

  // ── awaiting_configuration — legacy state, keep for backward compat ────────
  if (guidedState === "awaiting_configuration" && !negativeIntent) {
    const impliedTwo   = /\b(2|do|dono|two|to\b|too\b)\b/.test(text);
    const impliedThree = /\b(3|teen|three|tin)\b/.test(text);
    if (impliedTwo || impliedThree) {
      session._bhkType = impliedTwo ? "2" : "3";
      session.guidedState = "bhk_discussed";
      const bhkLabel = isHindi ? (impliedThree ? "teen BHK" : "do BHK") : (impliedThree ? "3 BHK" : "2 BHK");
      return T(
        `${project} has ${bhkLabel} in Compact and Classic layouts in Wings J and K with great views. Shall I share the pricing?`,
        `${project} mein ${bhkLabel} Compact aur Classic layout mein Wings J aur K mein available hai. Kimat bata doon?`
      );
    }
    session._configAsks = (session._configAsks || 0) + 1;
    if (session._configAsks >= 2) return null;
    return T(
      `Please tell me, are you interested in 2 BHK or 3 BHK?`,
      `Batayein, do BHK mein interest hai ya teen BHK mein?`
    );
  }

  // ── awaiting_contact_confirmation — PDF step 6: confirm callback number ─────
  if (guidedState === "awaiting_contact_confirmation") {
    // Any response (yes/no/number given) → close warmly. This is the final step.
    session.guidedState = "closed";
    return T(
      `Perfect! Our team will call you on this number within 24 hours to confirm the visit details. You will see the model apartment, all amenities, and the views live. It was really great speaking with you today. Have a wonderful day! Namaste.`,
      `Bilkul! Hamari team aapko is number par 24 ghante mein call karke visit ki details confirm kar legi. Aap model flat, saari amenities aur views live dekhenge. Bahut achha laga aaj aapase baat karke. Aapka din bahut shubh ho! Namaste.`
    );
  }

  // ── awaiting_callback_confirmation — legacy, redirect to site visit ────────
  if (guidedState === "awaiting_callback_confirmation") {
    if (positiveIntent) {
      session.guidedState = "site_visit_confirmed";
      return T(
        `I have noted your site visit request for ${project}. Our team will call you today to confirm the time. Thank you!`,
        `${project} ke liye site visit note kar li hai. Hamari team aaj call karegi time fix karne ke liye. Shukriya!`
      );
    }
    if (negativeIntent) {
      session.guidedState = "callback_declined";
      return T(
        `No problem. Thank you for your time. Have a great day!`,
        `Koi baat nahi. Aapka shukriya. Aapka din shubh ho!`
      );
    }
    return T(
      `Shall I book a site visit at ${project} for you today?`,
      `Kya main aaj ${project} ke liye site visit book kar doon?`
    );
  }
  if (guidedState === "awaiting_close_confirmation") {
    if (explicitFarewell) {
      session.guidedState = "closed";
      return T(
        `No problem. Have a great day. Goodbye.`,
        `Koi baat nahi. Aapka din shubh ho. Namaste.`
      );
    }
    session.guidedState = "awaiting_configuration";
    return T(
      `Of course! Are you interested in a two BHK or a three BHK at ${project}?`,
      `Zaroor! ${project} mein do BHK mein interest hai ya teen BHK mein?`
    );
  }
  if (positiveIntent) {
    session.guidedState = "open_discovery";
    return T(
      `I can help with price, location, or site visit details for ${project}. What would you like to know first?`,
      `Main ${project} ke baare mein rate, location ya site visit ki jaankari de sakti hoon. Pehle kya jaanna chahenge?`
    );
  }
  if (/location|where|near|connectivity|area|kahan|jagah|लोकेशन|कहाँ|कहां|जगह|स्थान|एड्रेस|पता|नज़दीक|पास में/.test(text)) {
    session.guidedState = "location_shared";
    return T(
      `${project} is in Pune with strong city connectivity. Would you like the pricing next?`,
      `${project} Pune mein hai, city connectivity bahut acchi hai. Ab rate bata doon?`
    );
  }
  if (/visit|site|schedule|appointment|callback|dekhna|milna|विज़िट|विजिट|साइट|देखना|मिलना|अपॉइंटमेंट/.test(text)) {
    session.guidedState = "awaiting_visit_day";
    return T(
      `Sure. I can note a site visit request. Which works better, today or tomorrow?`,
      `Zaroor. Main site visit request note kar sakti hoon. Aaj aayenge ya kal?`
    );
  }
  if (negativeIntent) {
    if (!guidedState || guidedState === "open_discovery" || guidedState === "location_shared") {
      session.guidedState = "awaiting_close_confirmation";
      return T(
        `I understand. Just before I let you go — would you like to know the pricing for ${project}? It only takes a moment.`,
        `Samajh gayi. Jaane se pehle ek kaam — ${project} ka rate ek baar sun lein, sirf ek minute lagega?`
      );
    }
    session.guidedState = "closed";
    return T(
      `No problem. Thank you for your time. Goodbye.`,
      `Koi baat nahi. Aapka shukriya. Namaste.`
    );
  }
  // If already in open_discovery and lead's reply is unclear, move conversation forward
  if (guidedState === "open_discovery") {
    session.guidedState = "awaiting_configuration";
    return T(
      `Are you interested in a two BHK or three BHK at ${project}? I can share the current pricing.`,
      `${project} mein do BHK ka interest hai ya teen BHK ka? Main rate bata sakti hoon.`
    );
  }
  // Generic fallback — only reached if guidedState is null and nothing matched
  session.guidedState = "open_discovery";
  return T(
    `I can help with price, location, or site visit details for ${project}. What would you like to know?`,
    `Main ${project} ke baare mein rate, location ya site visit ki jaankari de sakti hoon. Kya jaanna chahenge?`
  );
}

function isTerminalGuidedState(session) {
  // site_visit_confirmed removed — goodbye is now included in the confirmation response
  // itself (state jumps directly to "closed"), so we don't need a separate terminal check.
  return ["callback_confirmed", "callback_declined", "closed"].includes(session?.guidedState || "");
}

function shouldUseGuidedReply(session, userText = "") {
  const text = String(userText || "").toLowerCase().trim();
  const guidedState = session?.guidedState || null;

  // Terminal states — guided wraps up cleanly
  if (["callback_confirmed", "callback_declined", "site_visit_confirmed", "closed"].includes(guidedState)) return true;

  // Awaiting yes/no on site visit, contact confirmation, or legacy callback — guided handles
  if (["awaiting_callback_confirmation", "awaiting_site_visit", "awaiting_contact_confirmation"].includes(guidedState)) return true;

  // In bhk_discussed state — only intercept clear price/affirmation responses
  // For other questions (amenities, location, etc.), let LLM answer from KB
  if (guidedState === "bhk_discussed") {
    const isInfoRequest = /\b(bataiye|batao|bata|kya|kaise|kaisa|kaisi|kitna|kitne|kimat|price|cost|haan|ok|okay|theek|zaroor|bilkul|sure|yes|yes please|batao|bolo)\b|हाँ|जी|बताओ|ठीक|बिल्कुल/i.test(text);
    return isInfoRequest;
  }

  // price_discussed — guided handles positive/close, LLM handles further questions
  if (guidedState === "price_discussed" && /yes|haan|ji\b|sure|okay|ok|theek|bilkul|zaroor|ha\b/.test(text)) return true;

  // Clear goodbye / not interested — guided ends the call gracefully
  if (/\b(bye|goodbye|alvida|band karo|nahi chahiye|not interested|baad mein karana|later call|mujhe nahi chahiye|thank you|thanks|ok bye|ok thanks|theek hai ab|chalta hoon|chalti hoon|achha chalta|chalte hain)\b|थैंक\s*यू|धन्यवाद|शुक्रिया|अलविदा|चलते\s*हैं|चलता\s*हूँ|बाय/.test(text)) return true;

  // BHK / configuration questions — route to guided so LLM can't inject payment-plan tangents
  const hasBhkQuery = /(?:2|two|to\b|too\b|do\b|3|three|teen|4|four|char|1|one|ek)\s*(?:b\s*h\s*k|bhk|vhk|dhk)\b|(?:bhk|vhk|dhk)\b|configuration\b|flat\s+(?:size|type)|बीएचके|बी\.?एच\.?के/.test(text);
  if (hasBhkQuery) return true;

  // Everything else (amenities, location, possession date, open-ended Qs) → LLM with KB
  return false;
}

// ── LLM response — Groq fast path (50–150ms TTFT) with Ollama fallback ──────
async function getLLMResponse(session, userText) {
  const language = languageManager.getLanguage(session.callSid);
  session.history.push({ role: "user", content: userText });
  session.history = session.history.slice(-16);  // keep last 8 turns — longer calls need more context

  // Guided reply path — pure in-memory, ~0ms (handles pricing/BHK/location/callback)
  // Returns null when it wants LLM to take over (e.g. user is confused, not answering config question)
  if (shouldUseGuidedReply(session, userText)) {
    const reply = buildRuleBasedReply(session, userText);
    if (reply !== null) {
      session.history.push({ role: "assistant", content: reply });
      return reply;
    }
    // null → fall through to LLM
  }

  // Early-call affirmation shortcut — if the lead says "haan / ji / yes / okay"
  // as their very first response after the opening, they are confirming they can
  // talk — NOT asking a question. Respond with warm "kaise hain?" before qualifying.
  const userTurns = session.history.filter(h => h.role === "user").length;
  const isSimpleAffirmation = /^(haan|ha|yes|ji|okay|ok|theek|acha|accha|bilkul|zaroor|sure|haan ji|ha ji|theek hai|theek h|sahi|chal|chalo|bolo|batao|bol)[\.\!\s,]*$/i.test(userText.trim());
  if (userTurns === 1 && isSimpleAffirmation) {
    // Very first response to opening — Agni-style warm greeting before qualification
    const leadName = session.lead?.name?.split(" ")[0] || "ji";
    const reply = `Bahut badhiya ${leadName} ji! Aap kaise hain? Batayiye, aapki kya requirement hai — investment ke liye dekh rahe hain ya khud rehne ke liye?`;
    session.history.push({ role: "assistant", content: reply });
    return reply;
  }
  if (userTurns === 2 && isSimpleAffirmation) {
    // Second affirmation (e.g. "theek hoon" → still waiting for topic) — move to discovery
    const project = session.lead?.project || session.campaign?.name || "is project";
    const reply = `Achha, batayiye — ${project} ke baare mein kya jaanna chahenge? Price, location, ya BHK options?`;
    session.history.push({ role: "assistant", content: reply });
    return reply;
  }

  // Knowledge context — always fetch so LLM can answer any project question
  // Prefer pre-loaded KB in session, fallback to live fetch; cap at 4000 chars for GPT-4o-mini
  const knowledgeContext = (
    session.dynamicVariables?.knowledge_base ||
    (await getKnowledgeContext(session.campaign?.project_id || session.lead.project_id, userText))
  ).slice(0, 3500);  // 3500 chars — includes pricing section. (was 1500: pricing was cut off → agent said "not discussed")

  // Resolve language — _lockedLanguage (explicit user request) takes precedence
  const resolvedLanguage = session._lockedLanguage
    || ((language === "auto" || language === "auto-IN" || !language)
      ? (languageManager.getBaseLanguage(session.callSid) || "hi")
      : language);

  const systemPrompt = buildSystemPrompt(session.lead, knowledgeContext, resolvedLanguage, session.agentConfig || {}, session.qualification || {});

  // Web search — fires when user asks about a project not in KB
  const webSearchContext = await searchWebContext(userText, session.lead, knowledgeContext);

  // Send last 8 turns (16 messages) — needed for longer conversations (8+ min calls)
  // so agent remembers investment/BHK/budget stated early in the call.
  const historyContext = session.history.slice(-16).slice(0, -1);
  const currentTurn   = { role: "user", content: `[CURRENT — respond to this only]: ${userText}` };

  // Inject web search results as a system-level context block when available
  const searchBlock = webSearchContext
    ? { role: "system", content: `WEB SEARCH RESULTS for "${userText}" (use this to answer accurately, then pivot to our project):\n${webSearchContext}` }
    : null;
  const messages = [
    { role: "system", content: systemPrompt },
    ...historyContext,
    ...(searchBlock ? [searchBlock] : []),
    currentTurn,
  ];

  // ── Safe error body serializer — avoids circular JSON from stream responses ──
  // When responseType:'stream', err.response?.data is an IncomingMessage (TLSSocket)
  // which cannot be JSON.stringified. This helper detects streams and returns '[stream]'.
  function safeErrBody(data) {
    if (!data) return "{}";
    if (typeof data === "object" && typeof data.on === "function") return "[stream]";
    try { return JSON.stringify(data).slice(0, 200); } catch { return "[unstringifiable]"; }
  }

  // ── Streaming SSE helper — collects all chunks into a full reply string ──────
  // stream:true delivers first bytes sooner (lower TTFT) even when we wait for the
  // full response. For 90-token replies this saves ~80-150ms vs stream:false.
  //
  // CRITICAL: TCP chunks can split mid-line. We carry a `remainder` string so that
  // a JSON line broken across two chunks is re-assembled before parsing.
  // Without this, split lines are silently skipped → garbled / truncated text.
  async function collectStreamingReply(axiosResponse) {
    let fullText = "";
    let remainder = "";
    return new Promise((resolve, reject) => {
      axiosResponse.data.on("data", (chunk) => {
        // Prepend any incomplete line carried over from the previous chunk
        const text = remainder + chunk.toString("utf8");
        const lines = text.split("\n");
        // The last element may be an incomplete line — carry it to the next chunk
        remainder = lines.pop() || "";
        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          const data = line.slice(6).trim();
          if (data === "[DONE]") return;
          try {
            const parsed = JSON.parse(data);
            fullText += parsed.choices?.[0]?.delta?.content || "";
          } catch {}
        }
      });
      axiosResponse.data.on("end", () => {
        // Flush any remaining incomplete line
        if (remainder.startsWith("data: ")) {
          const data = remainder.slice(6).trim();
          if (data && data !== "[DONE]") {
            try {
              const parsed = JSON.parse(data);
              fullText += parsed.choices?.[0]?.delta?.content || "";
            } catch {}
          }
        }
        resolve(fullText.trim());
      });
      axiosResponse.data.on("error", reject);
    });
  }

  // ── Groq primary when LLM_PREFER_GROQ=true OR no OpenAI key ─────────────────
  // Groq llama-3.1-8b-instant: 50–150ms TTFT vs OpenAI 300–800ms.
  // Set LLM_PREFER_GROQ=true in Railway env to enable Groq-first routing.
  // Per-session override (browser test harness) takes precedence over env.
  const preferGroq = session.providerOverrides?.llm
    ? session.providerOverrides.llm === "groq"
    : process.env.LLM_PREFER_GROQ === "true";

  // ── Gemini (only when explicitly selected via provider override — e.g. the
  // browser test lab's bake-off dropdown). Gemini's chat/completions endpoint
  // is OpenAI-compatible, so it reuses the same collectStreamingReply parser. ──
  if (session.providerOverrides?.llm === "gemini" && process.env.GEMINI_API_KEY) {
    try {
      const t0 = Date.now();
      const response = await timed("gemini", () =>
        axios.post(
          "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
          {
            model: process.env.GEMINI_MODEL || "gemini-2.5-flash",
            messages,
            temperature: 0.3,
            max_tokens: 75,
            stream: true,
          },
          { headers: { Authorization: `Bearer ${process.env.GEMINI_API_KEY}` }, responseType: "stream", timeout: 8000 }
        )
      );
      const reply = stripFillerOpener(await collectStreamingReply(response));
      console.log(`[gemini] callSid=${session.callSid} latency=${Date.now()-t0}ms reply="${reply.slice(0,60)}"`);
      session.history.push({ role: "assistant", content: reply });
      const match = reply.match(/OUTCOME:({.*})/s);
      if (match) { try { session.outcome = JSON.parse(match[1]); } catch {} }
      return reply.replace(/OUTCOME:({.*})/s, "").trim();
    } catch (err) {
      const statusCode = err.response?.status;
      const errBody = safeErrBody(err.response?.data);
      console.warn(`[gemini] failed (HTTP ${statusCode || "?"}) falling back to OpenAI/Groq: ${err.message} — ${errBody}`);
    }
  }

  // ── OpenAI (primary unless preferGroq=true) ───────────────────────────────
  if (process.env.OPENAI_API_KEY && !preferGroq) {
    try {
      const t0 = Date.now();
      const response = await timed("openai", () =>
        axios.post(
          "https://api.openai.com/v1/chat/completions",
          {
            model: process.env.OPENAI_MODEL || "gpt-4.1",
            messages,
            temperature: 0.3,
            max_tokens: 75,   // ~15 words — forces LLM to be concise, prevents mid-word cut by TTS cap
            stream: true,    // streaming: first bytes arrive faster, lower TTFT
          },
          {
            headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}` },
            responseType: "stream",
            timeout: 8000,
          }
        )
      );
      const reply = stripFillerOpener(await collectStreamingReply(response));
      console.log(`[openai] callSid=${session.callSid} latency=${Date.now()-t0}ms model=${process.env.OPENAI_MODEL || "gpt-4.1"} reply="${reply.slice(0,60)}"`);
      session.history.push({ role: "assistant", content: reply });
      const match = reply.match(/OUTCOME:({.*})/s);
      if (match) { try { session.outcome = JSON.parse(match[1]); } catch {} }
      return reply.replace(/OUTCOME:({.*})/s, "").trim();
    } catch (err) {
      const statusCode = err.response?.status;
      const errBody = safeErrBody(err.response?.data);
      console.warn(`[openai] failed (HTTP ${statusCode || "?"}) falling back to Groq: ${err.message} — ${errBody}`);
    }
  }

  // ── Groq (primary when preferGroq=true, otherwise fallback) ──────────────
  if (process.env.GROQ_API_KEY) {
    try {
      const t0 = Date.now();
      const response = await timed("groq", () =>
        axios.post(
          "https://api.groq.com/openai/v1/chat/completions",
          {
            model: process.env.GROQ_MODEL || "llama-3.3-70b-versatile",
            messages,
            temperature: 0.2,
            max_tokens: 75,   // ~15 words — forces LLM to be concise, prevents mid-word cut by TTS cap
            stream: true,    // Groq streaming: even faster first-token delivery
          },
          {
            headers: { Authorization: `Bearer ${process.env.GROQ_API_KEY}` },
            responseType: "stream",
            timeout: 4000,
          }
        )
      );
      const reply = stripFillerOpener(await collectStreamingReply(response));
      console.log(`[groq] callSid=${session.callSid} latency=${Date.now()-t0}ms reply="${reply.slice(0,60)}"`);
      session.history.push({ role: "assistant", content: reply });
      const match = reply.match(/OUTCOME:({.*})/s);
      if (match) { try { session.outcome = JSON.parse(match[1]); } catch {} }
      return reply.replace(/OUTCOME:({.*})/s, "").trim();
    } catch (err) {
      const statusCode = err.response?.status;
      const data = err.response?.data;
      // Read actual error body from stream so we can diagnose 400/401/429 causes
      let errBody = "{}";
      if (data && typeof data.on === "function") {
        try {
          const chunks = [];
          await new Promise((res) => { data.on("data", c => chunks.push(c)); data.on("end", res); data.on("error", res); });
          errBody = Buffer.concat(chunks).toString().slice(0, 300);
        } catch { errBody = "[stream-read-failed]"; }
      } else {
        errBody = safeErrBody(data);
      }
      console.warn(`[groq] failed (HTTP ${statusCode || "?"}) falling back: ${err.message} — ${errBody}`);
    }
  }

  // ── OpenAI as last LLM resort when preferGroq=true but Groq failed ───────
  if (process.env.OPENAI_API_KEY && preferGroq) {
    try {
      const t0 = Date.now();
      const response = await timed("openai_fallback", () =>
        axios.post(
          "https://api.openai.com/v1/chat/completions",
          { model: process.env.OPENAI_MODEL || "gpt-4.1", messages, temperature: 0.3, max_tokens: 70, stream: true },
          { headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}` }, responseType: "stream", timeout: 8000 }
        )
      );
      const reply = stripFillerOpener(await collectStreamingReply(response));
      console.log(`[openai_fallback] callSid=${session.callSid} latency=${Date.now()-t0}ms reply="${reply.slice(0,60)}"`);
      session.history.push({ role: "assistant", content: reply });
      const match = reply.match(/OUTCOME:({.*})/s);
      if (match) { try { session.outcome = JSON.parse(match[1]); } catch {} }
      return reply.replace(/OUTCOME:({.*})/s, "").trim();
    } catch (err) {
      console.warn(`[openai_fallback] failed: ${err.message}`);
    }
  }

  // ── Last resort: rule-based reply ───────────────────────────────────────────
  {
    console.warn("[llm] all LLM paths failed, using guided fallback", { callSid: session.callSid, message: "no LLM available" });
    const reply = buildRuleBasedReply(session, userText);
    session.history.push({ role: "assistant", content: reply });
    return reply;
  }
}

async function getOpeningMessage(session) {
  const explicitOpening =
    session.campaign?.opening_line ||
    session.campaign?.openingLine ||
    session.campaign?.opening_text ||
    session.campaign?.opening_message ||
    session.lead?.opening_text ||
    process.env.DEFAULT_OPENING_TEXT ||
    "";

  // Use explicit opening if set, but cap it to the first 2 sentences so it
  // never runs longer than ~5 seconds. Long openings get cut off by impatient
  // callers — a brief greeting is more effective.
  // Interpolate template placeholders before normalizing
  const leadName = session.lead?.name || "ji";
  const projectName = session.lead?.project || session.campaign?.name || "hamare project";
  const interpolated = explicitOpening.trim()
    .replace(/\{[\s]*lead[\s_]*name[\s]*\}/gi, leadName)
    .replace(/\{[\s]*name[\s]*\}/gi, leadName)
    .replace(/\{[\s]*lead[\s]*\}/gi, leadName)
    .replace(/\[Lead Name\]/gi, leadName)
    .replace(/\{[\s]*project[\s_]*name[\s]*\}/gi, projectName)
    .replace(/\{[\s]*project[\s]*\}/gi, projectName)
    .replace(/\[Project Name\]/gi, projectName);

  const rawOpening = normalizeTtsText(interpolated);
  const opening = rawOpening
    ? (() => {
        // Allow up to 3 sentences, cap at 30 words.
        const sentences = rawOpening.split(/(?<=[.!?।])\s+/);
        const threeSentences = sentences.slice(0, 3).join(" ").trim();
        return capReplyWords(threeSentences, 30);
      })()
    : (() => {
        // No opening line configured — pick randomly from a pool of Maya/Agni-style openers.
        // Each variant: name check + company intro + project interest reference + permission ask.
        const agentName  = session.agentConfig?.agentName  || "Priya";
        const company    = session.agentConfig?.companyName || "Prophunt";
        const proj       = projectName !== "hamare project" ? projectName : "";
        const p          = proj || "hamare project";
        const lang       = session._lockedLanguage
          || normalizeLanguageToISO(session.lead?.language || "auto");

        const pools = {
          hi: [
            `Namaste ${leadName} ji! Main ${agentName} bol rahi hoon ${company} se. Aapne ${p} mein interest dikhaya tha — kya abhi do minute hain?`,
            `Hello ${leadName} ji, ${agentName} calling from ${company}. Aapka ${p} ke liye inquiry tha — abhi baat kar sakte hain?`,
            `${leadName} ji, namaste! Main ${agentName} hoon, ${company} se. ${p} ke baare mein thodi baat karni thi — convenient hai abhi?`,
            `Arre ${leadName} ji, namaste! ${agentName} here from ${company}. Aapne recently ${p} mein interest show kiya tha — kya thoda waqt hai?`,
            `Hi ${leadName} ji! Main ${agentName} bol rahi hoon ${company} ki taraf se. ${p} ke liye aapka enquiry tha — kya abhi baat ho sakti hai?`,
          ],
          mr: [
            `नमस्कार ${leadName}जी! मी ${agentName} बोलतेय, ${company} मधून. आपण ${p} मध्ये interest दाखवला होता — आत्ता दोन मिनिट बोलता येईल का?`,
            `नमस्कार! ${leadName}जी बोलत आहात का? मी ${agentName}, ${company} मधून. ${p} बद्दल बोलायचे होते — वेळ आहे का आत्ता?`,
            `हॅलो ${leadName}जी, मी ${agentName} बोलतेय ${company} कडून. ${p} साठी तुमची enquiry होती — आत्ता थोडं बोलता येईल का?`,
          ],
          en: [
            `Hello ${leadName}! This is ${agentName} calling from ${company}. You had shown interest in ${proj || "one of our properties"} — do you have a couple of minutes?`,
            `Hi ${leadName}, ${agentName} here from ${company}. I'm calling about your enquiry for ${proj || "our project"} — is now a good time to chat?`,
            `Good day ${leadName}! I'm ${agentName} from ${company}. You recently showed interest in ${proj || "a property with us"} — do you have two minutes?`,
          ],
        };

        const pool   = pools[lang] || pools.hi;
        const picked = pool[Math.floor(Math.random() * pool.length)];
        return capReplyWords(picked, 35);
      })();

  // Seed history so subsequent LLM turns have context of how the call started
  session.history.push({ role: "user",      content: "[CALL_STARTED]" });
  session.history.push({ role: "assistant", content: opening });
  session.history = session.history.slice(-12);
  return opening;
}

// ── Content-based echo detection ─────────────────────────────────────────────
// Old approach dropped EVERY transcript that arrived inside a fixed time window
// after the agent finished speaking (up to 5s for a long reply). Real callers
// often respond within a few hundred ms of the agent finishing — well within
// that window — so genuine speech was being silently discarded ("doesn't pick
// up what I'm saying", "words are cut"). Instead of trusting the clock alone,
// compare what was actually transcribed against what the agent just said: if
// most of the transcript's words appear in the agent's last line, it's almost
// certainly the agent's own voice reflecting back down the phone line — real
// replies essentially never restate the agent's sentence back to it.
function isLikelyEcho(transcriptText, session) {
  const lastAgentMsg = [...(session.history || [])].reverse().find(h => h.role === "assistant");
  if (!lastAgentMsg?.content) return false;
  const norm = (s) => s.toLowerCase().replace(/[.,!?।…"'\-]/g, " ").trim();
  const agentWords = new Set(norm(lastAgentMsg.content).split(/\s+/).filter(w => w.length > 2));
  const transWords = norm(transcriptText).split(/\s+/).filter(w => w.length > 2);
  if (!transWords.length || agentWords.size < 2) return false;
  const overlap = transWords.filter(w => agentWords.has(w)).length;
  return (overlap / transWords.length) >= 0.6;
}

function emotionFromContext(text = "", state = {}) {
  const lowered = text.toLowerCase();
  if (state.stage === "opening") return "warm";
  // Farewell / closing — warm and gracious
  if (/(bye|goodbye|thank you|thanks|dhanyawaad|shukriya|alvida|namaste|have a|good day|shubh|aapka din)/.test(lowered)) return "warm";
  // Excitement triggers — user expressing genuine interest or positives
  if (/(benefit|amenity|feature|offer|launch|badhiya|achha|accha|acha|wah|vah|pasand|sundar|शानदार|बढ़िया|अच्छा|वाह|पसंद|सुंदर|interest|good|great|nice|love|like|perfect|zaroor|bilkul|haan ji)/.test(lowered)) return "excited";
  // Empathy triggers — concerns, price sensitivity, hesitation, negative response
  if (/(price|budget|expensive|concern|issue|problem|sochna|baad|later|costly|mehenga|महंगा|सोचना|बाद में|परेशान|दिक्कत|theek nahi|nahi chahiye|nahi chahie|nahi|nahin|na\b)/.test(lowered)) return "empathetic";
  // Professional triggers — information requests, site visit, scheduling
  if (/(visit|schedule|book|callback|meeting|dekhna|site|confirm|date|time|slot|aana|aaun|आना|देखना|बुक|कब|location|kahan|possession|rera|loan|emi|floor|parking|amenities)/.test(lowered)) return "professional";
  // Default — warm baseline
  return "warm";
}

// Sarvam AI voice roster — female & male per language
// All voice IDs are lowercase as required by Sarvam API
const SARVAM_VOICE_MAP = {
  en: { female: "priya",  male: "shubh"  },  // English
  hi: { female: "ritu",   male: "rahul"  },  // Hindi
  mr: { female: "roopa",  male: "anand"  },  // Marathi
  ta: { female: "kavya",  male: "kavya"  },  // Tamil  (no dedicated male — kavya works)
  te: { female: "kavya",  male: "vijay"  },  // Telugu
  pa: { female: "simran", male: "simran" },  // Punjabi (no dedicated male)
  bn: { female: "shreya", male: "shreya" },  // Bengali (no dedicated male)
  gu: { female: "priya",  male: "shubh"  },  // Gujarati — fall back to EN voices
  kn: { female: "priya",  male: "shubh"  },  // Kannada  — fall back to EN voices
  ml: { female: "priya",  male: "shubh"  },  // Malayalam — fall back to EN voices
};

// Split reply into natural sentence chunks for streaming delivery
function splitIntoSentences(text) {
  // Split on Hindi/English sentence endings: . ! ? । and ellipsis
  const parts = text.split(/(?<=[.!?।…])\s+/).map(s => s.trim()).filter(Boolean);
  if (parts.length <= 1) return [text];
  // Merge sentences that are too short (< 6 words) with the next one
  const merged = [];
  let buf = '';
  for (const s of parts) {
    buf = buf ? buf + ' ' + s : s;
    if (buf.split(/\s+/).length >= 6 || s === parts[parts.length - 1]) {
      merged.push(buf.trim());
      buf = '';
    }
  }
  if (buf) merged.push(buf.trim());
  return merged.length ? merged : [text];
}

// ── Strip forced/habitual filler openers ("Haan...", "Achha...", "Hmm...") ───
// Applied regardless of which system prompt produced the reply — the auto-generated
// fallback prompt AND any custom prompt saved in the dashboard could both instruct
// (or habitually drift toward) opening every turn with the same filler word, which
// reads as a robotic tic once a caller notices the pattern after a few turns.
// Only strips a filler word immediately followed by "..."/"…"/"," — i.e. the exact
// "word-then-pause" shape a forced-filler instruction produces — so a genuine
// affirmation like "Haan, bilkul sahi bola aapne" (no pause) is left untouched.
// Only "..."/"…" count as the pause marker — NOT a comma. A comma after a filler
// word ("Haan, bilkul sahi bola aapne") is a normal sentence continuation, not the
// robotic "word-then-pause" tic the forced-filler instruction produced.
const FILLER_OPENER_RE = /^(?:haan|ha|hey|hmm+|ac{1,2}h{1,2}a|samjha|samajha|dekhiye|dekho|bilkul|theek(?:\s+hai)?|arre+y?\s*wah|sure|right|okay|ok|well|bghaa|mm+-?hmm+|हाँ|हां|अच्छा|हम्म+|समझ[ाी]|देखिए|बिल्कुल|ठीक(?:\s+है)?|अरे\s*वाह)[\s]*(?:\.\.\.|…|-{2,})+\s*/iu;
function stripFillerOpener(text) {
  if (!text) return text;
  const stripped = text.replace(FILLER_OPENER_RE, "");
  if (!stripped || stripped === text) return text;
  return stripped.charAt(0).toUpperCase() + stripped.slice(1);
}

// Hard-cap reply to MAX_WORDS words to prevent long TTS audio.
// ElevenLabs Hindi TTS speaks at ~1.4 words/sec (NOT 2.5 — that's English).
// 12 words × (1/1.4) ≈ 8.6s audio — acceptable for a phone turn.
// Ends with "." so TTS reads it as a complete sentence, not mid-cut.
function capReplyWords(text, maxWords = 12) {
  const words = text.trim().split(/\s+/);
  if (words.length <= maxWords) return text;
  // Strip trailing punctuation from the last kept word, then add period
  const trimmed = words.slice(0, maxWords).join(" ").replace(/[,;—–\s]+$/, "");
  return trimmed + ".";
}

// Stream reply sentence-by-sentence — lead hears first sentence ~200ms sooner
async function synthesizeAndStreamReply(ws, session, fullText) {
  // Hard word-cap before anything else — prevents long audio chunks.
  // ElevenLabs Hindi TTS: ~1.4 words/sec → 12 words ≈ 8.6s audio.
  const capped = capReplyWords(fullText, parseInt(process.env.TTS_MAX_WORDS || "18", 10));

  // Allow up to 3 sentences — lets the agent speak naturally with flow.
  // Word cap above (35 words) keeps total audio under ~10s which is fine for phone calls.
  const allSentences = splitIntoSentences(capped);
  const sentences = allSentences.slice(0, 3);
  let firstSent = false;
  let lastKnownGeneration = session.telephony?.outGeneration || 0;

  for (const sentence of sentences) {
    if (!sentence || session.closed || session.telephony?.hangupScheduled) break;

    // ── Barge-in guard: if outGeneration changed since the last send, a barge-in
    // fired during our wait and cleared the audio queue. Stop streaming — continuing
    // would send the next sentence onto a cleared channel and produce a skip/jump.
    if (firstSent && (session.telephony?.outGeneration || 0) !== lastKnownGeneration) {
      console.log(`[synthesize] barge-in detected mid-stream, stopping early callSid=${session.callSid}`);
      break;
    }

    const audio = await synthesizeSpeech(session, sentence);
    if (!audio) continue;

    if (!firstSent) {
      clearEnablexMedia(ws, session);  // cancel any previous audio
      firstSent = true;
    }

    if (ws.readyState !== WebSocket.OPEN) break;
    await recordAgentAudio(session, audio, "agent-reply");
    sendEnablexMedia(ws, session, audio, "streaming-sentence");
    // Snapshot generation right after send — sendEnablexMedia increments it
    lastKnownGeneration = session.telephony?.outGeneration || 0;

    // Wait for THIS sentence's playback to finish (use its own duration, not last audio's)
    // lastPlaybackMs is updated inside sendEnablexMedia for the just-sent chunk
    const playMs = session.telephony?.lastPlaybackMs || 800;
    await new Promise(r => setTimeout(r, Math.min(playMs + 80, 4000)));  // cap at 4s
  }

  return firstSent;
}

// Known Sarvam voice IDs (all lowercase)
const SARVAM_KNOWN_VOICES = new Set(["priya","shubh","ritu","rahul","roopa","anand","kavya","simran","shreya","vijay"]);

// Infer gender from a Sarvam voice name selected in the dashboard
function inferGenderFromVoiceName(name = "") {
  const male = ["shubh", "rahul", "anand", "vijay"];
  return male.includes(name.toLowerCase()) ? "male" : "female";
}

// Convert an Indian-comma-formatted or plain integer string to spoken words.
// Returns null if the number is outside the lakh/crore range or not clean.
function toIndianWords(numStr) {
  const n = parseInt(String(numStr).replace(/,/g, ""), 10);
  if (isNaN(n) || n < 0) return null;
  if (n >= 1e9) return null; // above 100 crore — leave alone
  if (n >= 1e7) {
    const crInt = Math.floor(n / 1e7);
    const rem   = n - crInt * 1e7;
    const lakhs = Math.round(rem / 1e5);
    if (lakhs === 0) return `${crInt} crore`;
    if (crInt  === 0) return `${lakhs} lakh`;
    return `${crInt} crore ${lakhs} lakh`;
  }
  if (n >= 1e5) {
    const lkInt = Math.floor(n / 1e5);
    const rem   = n - lkInt * 1e5;
    const th    = Math.round(rem / 1000);
    if (th    === 0) return `${lkInt} lakh`;
    if (lkInt === 0) return `${th} hazaar`;
    return `${lkInt} lakh ${th} hazaar`;
  }
  if (n >= 1000) {
    const th = n / 1000;
    if (Number.isInteger(th)) return `${th} hazaar`;
  }
  return null;
}

// Normalise text before TTS: expand abbreviations and fix known mispronunciations
function normalizeTtsText(text) {
  return (text || "")
    // ── Brand names ─────────────────────────────────────────────────────
    .replace(/\bProphunt\b/gi,   "Prop Hunt")
    .replace(/\bProphunts?\b/gi, "Prop Hunt")
    .replace(/\bprop-hunt\b/gi,  "Prop Hunt")
    .replace(/\bArthaleads?\b/gi, "Artha Leads")

    // ── Currency: ₹ / Rs. / INR → "rupaye" ─────────────────────────────
    .replace(/₹\s*/g,     "rupaye ")
    .replace(/Rs\.\s*/gi, "rupaye ")
    .replace(/\bRs\b/gi,  "rupaye")
    .replace(/\bINR\b/g,  "rupaye")

    // ── Area units ──────────────────────────────────────────────────────
    .replace(/sq\.?\s*ft\.?/gi,              "square feet")
    .replace(/\bsqft\b/gi,                   "square feet")
    .replace(/sq\.?\s*f(?:eet|oot)\.?/gi,    "square feet")
    .replace(/sq\.?\s*m(?:t|tr|eter)?\.?/gi, "square meter")
    .replace(/\bsqmt\b/gi,                   "square meter")
    .replace(/\bsq\b/gi,                     "square")

    // ── Large Indian number suffixes ─────────────────────────────────────
    // e.g. "1.5Cr" → "1.5 crore",  "80L" / "80 lac" → "80 lakh"
    .replace(/\b(\d+(?:\.\d+)?)\s*[Cc]r\.?\b/g,           "$1 crore")
    .replace(/\b(\d+(?:\.\d+)?)\s*[Ll](?:ac|akh)?\.?\b/g, "$1 lakh")

    // ── Indian comma-format numbers → spoken Indian words ─────────────────
    // Matches 1,00,000 / 50,00,000 / 1,00,00,000 / 1,50,000 etc.
    // Pattern: 1–2 digits, then groups of 2 digits, final group of 3 digits.
    // Excludes plain western thousands like 1,234 (toIndianWords returns null → kept as-is).
    .replace(/\b(\d{1,2}(?:,\d{2})*,\d{3})\b/g, m => toIndianWords(m) || m)

    // ── Number ranges: "54–70" / "54-70" / "54 to 70" → "54 se 70" ────────
    // ElevenLabs reads en-dash as "minus" — replace with natural Hindi "se"
    .replace(/(\d+(?:\.\d+)?)\s*[–—-]\s*(\d+(?:\.\d+)?)\s*(lakh|crore|lac|लाख|करोड़|L\b|Cr\b)/gi,
             (_, a, b, unit) => `${a} se ${b} ${unit}`)
    .replace(/(\d+(?:\.\d+)?)\s*[–—]\s*(\d+(?:\.\d+)?)/g, "$1 se $2")

    // ── Percentage ───────────────────────────────────────────────────────
    .replace(/(\d)\s*%/g, "$1 percent")

    // ── Floor notation: G+12 → "Ground plus 12" ─────────────────────────
    .replace(/\bG\+(\d+)\b/g, "Ground plus $1")

    // ── BHK: replace with Hindi phonetic so ElevenLabs doesn't say "BHKA" ──
    .replace(/\b(\d)\s*BHK\b/gi, (_, n) => `${n} बी एच के`)  // "2BHK" → "2 बी एच के"
    .replace(/\bBHK\b/gi,        "बी एच के")                  // standalone "BHK"
    .replace(/\b(\d)\s*RK\b/gi,  (_, n) => `${n} आर के`)     // "1RK" → "1 आर के"
    .replace(/\bRK\b/gi,         "आर के")

    // ── Common Indian RE abbreviations → Hindi phonetic ──────────────────
    .replace(/\bRERA\b/g,  "रेरा")
    .replace(/\bEMI\b/g,   "ईएमआई")
    .replace(/\bPCMC\b/g,  "पी.सी.एम.सी.")
    .replace(/\bMC\b/g,    "एम.सी.")
    .replace(/\bBMC\b/g,   "बी.एम.सी.")
    .replace(/\bNOC\b/g,   "एन.ओ.सी.")
    .replace(/\bOC\b/g,    "ओ.सी.")
    .replace(/\bCC\b/g,    "सी.सी.")
    .replace(/\bPLC\b/g,   "पी.एल.सी.")
    .replace(/\bGST\b/g,   "जीएसटी")
    .replace(/\bTDS\b/g,   "टीडीएस")
    .replace(/\bLTV\b/g,   "एलटीवी")

    // ── Direction abbreviations ──────────────────────────────────────────
    .replace(/\bE-W\b/gi,  "east west")
    .replace(/\bN-S\b/gi,  "north south");
}

async function synthesizeSpeechSarvam(text, voiceId, lang, modelOverride = null) {
  const sarvamKey = process.env.SARVAM_API_KEY;
  if (!sarvamKey) return null;
  const langCode = SARVAM_LANG_MAP[lang] || "hi-IN";
  const speaker = voiceId || "meera";
  const model = modelOverride || process.env.SARVAM_TTS_MODEL || "bulbul:v3";
  const t0 = Date.now();
  try {
    const response = await timed("tts_sarvam", () =>
      axios.post(
        `${process.env.SARVAM_API_URL || "https://api.sarvam.ai"}/text-to-speech`,
        {
          inputs: [text],
          target_language_code: langCode,
          speaker,
          model,
          pace: parseFloat(process.env.SARVAM_TTS_PACE || "1.0"),
          sample_rate: 8000,
          enable_preprocessing: true,
        },
        {
          headers: {
            "api-subscription-key": sarvamKey,
            "Content-Type": "application/json",
          },
          timeout: parseInt(process.env.TTS_REQUEST_TIMEOUT_MS || "20000", 10),
        }
      )
    );
    const audios = response.data?.audios || [];
    if (!audios.length) return null;
    console.log(`[tts-sarvam] latency=${Date.now()-t0}ms speaker=${speaker} lang=${langCode}`);
    return Buffer.from(audios[0], "base64");
  } catch (err) {
    const body = err.response?.data ? JSON.stringify(err.response.data).slice(0, 300) : "";
    console.warn(`[tts-sarvam] failed (${Date.now()-t0}ms): ${err.message}${body ? " body=" + body : ""}`);
    return null;
  }
}

// Gemini native TTS — generateContent with responseModalities:["AUDIO"] returns
// raw PCM (24kHz, 16-bit, mono) as base64, which we wrap in a WAV header so the
// rest of the pipeline (resamplePcm16, toEnablexMuLawChunks) handles it uniformly.
const GEMINI_TTS_SAMPLE_RATE = 24000;
async function synthesizeSpeechGemini(text, voiceName, emotion) {
  if (!process.env.GEMINI_API_KEY) return null;
  const t0 = Date.now();
  // Gemini TTS takes style instructions as natural language rather than numeric
  // sliders — prepend a short tone cue so emotion actually affects the delivery.
  const toneCue = {
    warm: "Say this warmly and welcomingly: ",
    excited: "Say this with genuine enthusiasm and energy: ",
    empathetic: "Say this gently and reassuringly: ",
    professional: "Say this clearly and confidently: ",
  }[emotion] || "Say this naturally, in a warm conversational tone: ";
  try {
    const response = await timed("tts_gemini", () =>
      axios.post(
        `https://generativelanguage.googleapis.com/v1beta/models/${process.env.GEMINI_TTS_MODEL || "gemini-2.5-flash-preview-tts"}:generateContent`,
        {
          contents: [{ parts: [{ text: toneCue + text }] }],
          generationConfig: {
            responseModalities: ["AUDIO"],
            speechConfig: {
              voiceConfig: { prebuiltVoiceConfig: { voiceName: voiceName || "Kore" } },
            },
          },
        },
        {
          headers: { "x-goog-api-key": process.env.GEMINI_API_KEY, "Content-Type": "application/json" },
          timeout: parseInt(process.env.TTS_REQUEST_TIMEOUT_MS || "20000", 10),
        }
      )
    );
    const part = response.data?.candidates?.[0]?.content?.parts?.[0];
    const b64 = part?.inlineData?.data || part?.inline_data?.data;
    if (!b64) return null;
    const pcm = Buffer.from(b64, "base64");
    console.log(`[tts-gemini] latency=${Date.now()-t0}ms voice=${voiceName || "Kore"}`);
    return createWavBuffer(pcm, GEMINI_TTS_SAMPLE_RATE);
  } catch (err) {
    const body = err.response?.data ? JSON.stringify(err.response.data).slice(0, 300) : "";
    console.warn(`[tts-gemini] failed (${Date.now()-t0}ms): ${err.message}${body ? " body=" + body : ""}`);
    return null;
  }
}

// ── Circuit breaker for the TTS microservice fallback ────────────────────────
// Module-level (not per-session) because the underlying cause — a bad API key
// or an exhausted account-wide rate limit — affects every concurrent call, not
// just one. Without this, a single misconfigured credential caused every
// filler/greeting/nudge/goodbye call across every active session to keep
// retrying a guaranteed-to-fail endpoint with zero backoff.
let ttsMicroserviceCooldownUntil  = 0;
let ttsMicroserviceCooldownReason = "";
function ttsMicroserviceOnCooldown() {
  return Date.now() < ttsMicroserviceCooldownUntil;
}
function tripTtsMicroserviceCircuitBreaker(status) {
  // 401/403 = the credential itself is wrong — won't self-heal, so cool down
  // longer. 429 = rate limit, likely to reset soon — shorter cooldown.
  const cooldownMs = (status === 401 || status === 403) ? 120000 : 15000;
  ttsMicroserviceCooldownUntil  = Date.now() + cooldownMs;
  ttsMicroserviceCooldownReason = `HTTP ${status}`;
  console.warn(`[tts] circuit breaker OPEN for ${cooldownMs}ms (reason: HTTP ${status}) — skipping microservice fallback calls until cooldown expires`);
}

async function synthesizeSpeech(session, text) {
  const normalizedText = normalizeTtsText(text);
  // gender: from campaign (set by dashboard voice selection) → lead → default female
  const gender = session.campaign?.voice_gender || session.lead?.voice_gender || "female";

  // Language-detected voice ID pattern from language-manager (e.g. "hi_female_01")
  const resolvedVoiceId = session.campaign?.voice_id || languageManager.resolveVoice(session.callSid, gender);

  const language = languageManager.getLanguage(session.callSid);
  const lang = languageManager.getBaseLanguage(session.callSid) || "hi";

  let voiceId;
  if (SARVAM_KNOWN_VOICES.has(resolvedVoiceId?.toLowerCase())) {
    voiceId = SARVAM_VOICE_MAP[lang]?.[gender] || SARVAM_VOICE_MAP["en"][gender] || "priya";
  } else if (/^([a-z]{2})_(male|female)_\d{2}$/i.test(resolvedVoiceId)) {
    voiceId = SARVAM_VOICE_MAP[lang]?.[gender] || SARVAM_VOICE_MAP["en"][gender] || "priya";
  } else {
    voiceId = resolvedVoiceId || "priya";
  }
  voiceId = voiceId.toLowerCase();

  // Per-session overrides (browser test harness) — voice ID kept case-sensitive
  // because ElevenLabs voice IDs are case-sensitive; Sarvam speakers are lowercase anyway.
  if (session.providerOverrides?.ttsVoice) voiceId = session.providerOverrides.ttsVoice;

  const ttsProvider = (session.providerOverrides?.tts || process.env.TTS_PROVIDER || "elevenlabs").toLowerCase();

  // ── Sarvam Bulbul TTS (primary when TTS_PROVIDER=sarvam, default) ─────────
  if (ttsProvider === "sarvam" || ttsProvider === "bulbul") {
    const sarvamModel = /^bulbul:/.test(session.providerOverrides?.ttsModel || "") ? session.providerOverrides.ttsModel : null;
    const audio = await synthesizeSpeechSarvam(normalizedText, voiceId, lang, sarvamModel);
    if (audio) return audio;
    console.warn("[tts] Sarvam failed, falling back to microservice");
  }

  const emotion = emotionFromContext(text, { stage: session.stage });

  // ── Gemini TTS (only when explicitly selected via provider override) ─────
  if (ttsProvider === "gemini") {
    const geminiVoice = session.providerOverrides?.ttsVoice || "Kore";
    const audio = await synthesizeSpeechGemini(normalizedText, geminiVoice, emotion);
    if (audio) return audio;
    console.warn("[tts] Gemini failed, falling back to microservice");
  }

  // ── Microservice fallback (handles ElevenLabs or local TTS) ──────────────
  if (ttsMicroserviceOnCooldown()) {
    console.warn(`[tts] microservice on cooldown (${ttsMicroserviceCooldownReason}) — skipping call callSid=${session.callSid}`);
    return null;
  }
  try {
    const response = await timed("tts", () =>
      axios.post(
        `${config.services.tts}/synthesize`,
        {
          text: normalizedText,
          voice_id: voiceId,
          language,
          gender,
          emotion,
          context: { stage: session.stage, lead_status: session.outcome?.status || session.lead.status || "new" },
        },
        { responseType: "arraybuffer", timeout: parseInt(process.env.TTS_REQUEST_TIMEOUT_MS || "25000", 10) }
      )
    );
    return Buffer.from(response.data);
  } catch (error) {
    const status = error.response?.status;
    console.warn("[tts] microservice synthesis failed", {
      callSid: session.callSid,
      voiceId,
      language,
      message: error.message,
      status,
    });
    // 401/403 = the credential is wrong and will NOT self-heal on retry; 429 = rate
    // limited. Retrying either on every single filler/greeting/nudge call across
    // every session (as before) hammered a guaranteed-to-fail endpoint hundreds of
    // times per second and stalled real turns for a minute-plus. Trip a cooldown
    // so subsequent calls fail fast instead of piling up.
    if (status === 401 || status === 403 || status === 429) {
      tripTtsMicroserviceCircuitBreaker(status);
    }
    return null;
  }
}

async function persistSession(session) {
  const serializable = { ...session, startedAt: session.startedAt, updatedAt: nowIso() };
  // Remove non-serializable objects — WebSocket and timer have circular refs that break JSON.stringify
  delete serializable.timer;
  delete serializable.deepgramWs;      // WebSocket → TLSSocket → HTTPParser (circular)
  delete serializable._mediaWs;        // media WebSocket (circular) — kept for STT switches
  delete serializable.agniBridge;      // LiveKit room refs (circular)
  delete serializable.inboundAudio;    // Buffers can be large — not needed in Redis
  delete serializable.recordings;      // PCM buffer arrays — not needed in Redis
  try {
    await redis.set(`session:${session.callSid}`, JSON.stringify(serializable), "EX", Math.ceil(config.callTimeoutMs / 1000));
  } catch (err) {
    console.warn(`[persist] JSON.stringify failed callSid=${session.callSid}: ${err.message}`);
  }
}

function safeRecordingId(callSid) {
  return String(callSid || crypto.randomUUID()).replace(/[^a-zA-Z0-9_.-]/g, "_");
}

function writeWavFile(filePath, pcm16Buffer, sampleRate = 16000) {
  fs.writeFileSync(filePath, createWavBuffer(pcm16Buffer, sampleRate));
}

function createWavBuffer(pcm16Buffer, sampleRate = 16000) {
  const header = Buffer.alloc(44);
  const byteRate = sampleRate * 2;
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcm16Buffer.length, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(1, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(2, 32);
  header.writeUInt16LE(16, 34);
  header.write("data", 36);
  header.writeUInt32LE(pcm16Buffer.length, 40);
  return Buffer.concat([header, pcm16Buffer]);
}

function ensureWavBuffer(audioBuffer, sampleRate = 16000) {
  if (!audioBuffer?.length) return audioBuffer;
  return audioBuffer.subarray(0, 4).toString("ascii") === "RIFF" ? audioBuffer : createWavBuffer(audioBuffer, sampleRate);
}

function recordingUrl(callSid, fileName) {
  return `${getConfiguredPublicBaseUrl()}/recordings/${safeRecordingId(callSid)}/${fileName}`;
}

function getRecordingState(session) {
  if (!session.recording) {
    const recordingId = safeRecordingId(session.callSid);
    const dir = path.join(config.recordingsDir, recordingId);
    fs.mkdirSync(dir, { recursive: true });
    session.recording = {
      id: recordingId,
      dir,
      callerPcmPath: path.join(dir, "caller.pcm"),
      agentPcmPath: path.join(dir, "agent.pcm"),
      mixedPcmPath: path.join(dir, "mixed.pcm"),
      timelinePath: path.join(dir, "timeline.json"),
      timeline: [],
      startedAt: nowIso(),
    };
  }
  return session.recording;
}

async function appendRecordingAudio(session, speaker, pcm16Buffer, label = "audio") {
  if (!session || !pcm16Buffer?.length) return null;
  const recording = getRecordingState(session);
  const targetPath = speaker === "agent" ? recording.agentPcmPath : recording.callerPcmPath;
  await fs.promises.appendFile(targetPath, pcm16Buffer);
  await fs.promises.appendFile(recording.mixedPcmPath, pcm16Buffer);
  recording.timeline.push({
    speaker,
    label,
    timestamp: nowIso(),
    bytes: pcm16Buffer.length,
    duration_ms: Math.round((pcm16Buffer.length / 2 / 16000) * 1000),
  });
  return recording;
}

async function recordCallerAudio(session, pcm16Buffer, label = "caller-media") {
  return appendRecordingAudio(session, "caller", pcm16Buffer, label);
}

async function recordAgentAudio(session, wavBuffer, label = "agent-media") {
  if (!wavBuffer?.length) return null;
  const { pcm, sampleRate } = parseWavInfo(wavBuffer);
  return appendRecordingAudio(session, "agent", resamplePcm16(pcm, sampleRate, 16000), label);
}

async function uploadRecordingToCloudinary(filePath, callSid) {
  if (!process.env.CLOUDINARY_CLOUD_NAME || !fs.existsSync(filePath)) return null;
  try {
    const result = await cloudinary.uploader.upload(filePath, {
      resource_type: "video",
      folder: "call-recordings",
      public_id: `${callSid}/mixed`,
      overwrite: true,
    });
    return result.secure_url;
  } catch (err) {
    console.error("[cloudinary] upload failed:", err.message);
    return null;
  }
}

async function finalizeRecording(session) {
  if (!session?.recording || session.recording.finalized) {
    return session?.recordings || null;
  }
  const recording = session.recording;
  const files = {
    caller: path.join(recording.dir, "caller.wav"),
    agent: path.join(recording.dir, "agent.wav"),
    mixed: path.join(recording.dir, "mixed.wav"),
  };
  const callerPcm = fs.existsSync(recording.callerPcmPath) ? fs.readFileSync(recording.callerPcmPath) : Buffer.alloc(0);
  const agentPcm = fs.existsSync(recording.agentPcmPath) ? fs.readFileSync(recording.agentPcmPath) : Buffer.alloc(0);
  const mixedPcm = fs.existsSync(recording.mixedPcmPath) ? fs.readFileSync(recording.mixedPcmPath) : Buffer.alloc(0);
  if (callerPcm.length) writeWavFile(files.caller, callerPcm);
  if (agentPcm.length) writeWavFile(files.agent, agentPcm);
  if (mixedPcm.length) writeWavFile(files.mixed, mixedPcm);
  await fs.promises.writeFile(recording.timelinePath, JSON.stringify(recording.timeline, null, 2));
  recording.finalized = true;

  // Persist mixed WAV in Redis so it survives container restarts / redeploys.
  // Railway's filesystem is ephemeral — local file URLs break after every deploy.
  // Redis TTL: 30 days (same as call log retention).
  if (mixedPcm.length) {
    // Recordings are served from Cloudinary / local disk — no Redis caching needed.
    // Storing audio in Redis was filling the 256MB free-tier limit in <50 calls.
  }

  session.recordingPath = mixedPcm.length ? recordingUrl(session.callSid, "mixed.wav") : null;
  session.recordings = {
    caller_path: callerPcm.length ? files.caller : null,
    agent_path: agentPcm.length ? files.agent : null,
    mixed_path: mixedPcm.length ? files.mixed : null,
    timeline_path: recording.timelinePath,
    caller_url: callerPcm.length ? recordingUrl(session.callSid, "caller.wav") : null,
    agent_url: agentPcm.length ? recordingUrl(session.callSid, "agent.wav") : null,
    mixed_url: mixedPcm.length ? recordingUrl(session.callSid, "mixed.wav") : null,
    timeline: recording.timeline,
  };
  return session.recordings;
}

async function endCall(session, finalStatus = "completed") {
  if (session.closed) return;
  session.closed = true;
  session.status = finalStatus;
  session.endedAt = nowIso();

  // Close Deepgram streaming WebSocket for this call
  closeDeepgramStream(session);

  // Disconnect Agni LiveKit bridge if active
  if (session.agniBridge) {
    session.agniBridge.disconnect().catch(() => {});
    session.agniBridge = null;
  }

  await finalizeRecording(session);
  if (session.recordings?.mixed_path) {
    const cloudUrl = await uploadRecordingToCloudinary(session.recordings.mixed_path, session.callSid);
    if (cloudUrl) {
      session.recordingPath = cloudUrl;
      session.recordings.mixed_url = cloudUrl;
      // Container filesystem is ephemeral — delete local WAV + PCM files after
      // successful Cloudinary upload to prevent disk exhaustion on long-running deploys.
      const rec = session.recording;
      for (const p of [
        session.recordings.caller_path,
        session.recordings.agent_path,
        session.recordings.mixed_path,
        rec?.callerPcmPath, rec?.agentPcmPath, rec?.mixedPcmPath,
      ]) {
        if (p) fs.unlink(p, () => {});
      }
    }
  }
  const durationSec = Math.max(1, Math.round((Date.now() - session.startedTs) / 1000));
  callsTotal.labels(finalStatus).inc();
  callDuration.observe(durationSec);
  const outcome = {
    ...(session.outcome || {
    status: finalStatus,
    call_duration_sec: durationSec,
    transcript_summary: session.history.map((item) => `${item.role}: ${item.content}`).join(" | ").slice(0, 1000),
    site_visit_scheduled: false,
    callback_date: null,
    lead_temperature: "warm",
    qualification: { bhk: "", budget_range: "", purpose: "", timeline: "" },
    full_transcript: JSON.stringify(session.history),
    }),
    status: session.outcome?.status || finalStatus,
    call_duration_sec: durationSec,
    full_transcript: session.outcome?.full_transcript || JSON.stringify(session.history),
    recording_url: session.recordingPath || session.outcome?.recording_url || null,
  };
  try {
    await pushToCRM(session.lead.id, { ...outcome, call_duration_sec: durationSec });
  } catch {}
  await persistCallLog(session, { ...outcome, call_duration_sec: durationSec }, durationSec, finalStatus);
  await persistSession(session);
  // Notify live feed subscribers that the call has ended, then close their connections
  broadcastLiveEvent(session, { type: "call_status", status: "ended" });
  if (session.liveSubscribers) {
    for (const sub of session.liveSubscribers) {
      try { if (sub.readyState === WebSocket.OPEN) sub.close(1000, "call ended"); } catch (_) {}
    }
    session.liveSubscribers.clear();
  }
  sessions.delete(session.callSid);
  languageManager.clear(session.callSid);
}

// Normalize language string from dashboard ("Hindi", "English", "hi", "en", etc.) to ISO code
function normalizeLanguageToISO(lang = "") {
  const map = {
    "hindi": "hi", "english": "en", "marathi": "mr", "tamil": "ta",
    "telugu": "te", "bengali": "bn", "punjabi": "pa", "gujarati": "gu",
    "kannada": "kn", "malayalam": "ml", "auto": "auto", "multilingual": "auto",
  };
  const lower = String(lang || "").toLowerCase().split("-")[0];
  return map[lower] || lower || "auto";
}

function createSession(lead, campaign = {}, callSid = crypto.randomUUID()) {
  // Support both "language_preference" (CRM leads) and "language" (dashboard test calls)
  const rawLang = lead.language_preference || lead.language || campaign.language || "auto";
  const preferredLanguage = normalizeLanguageToISO(rawLang);
  languageManager.initialize(callSid, preferredLanguage);
  const session = {
    callSid,
    lead,
    campaign,
    history: [],
    status: "initiated",
    stage: "opening",
    startedAt: nowIso(),
    startedTs: Date.now(),
    closed: false,
    outcome: null,
    recordingPath: null,
    telephony: null,
    pendingGreetingAudio: null,
    dynamicVariables: null,  // set by /call/dial from dashboard KB payload
    _ttsCache: {},           // pre-warmed audio for common phrases
  };
  session.timer = setTimeout(() => endCall(session, "timeout"), config.callTimeoutMs);
  sessions.set(callSid, session);
  return session;
}

// Pre-warm TTS for the most frequent agent phrases so they play from cache instantly.
// Called after session creation — runs in background, doesn't block the dial response.
async function prewarmTTSCache(session) {
  const lang = languageManager.getBaseLanguage(session.callSid) || "hi";
  const phrases = lang === "hi" ? [
    "Ek second.",
    "Samajh gaya.",
    "Bilkul.",
    "Koi baat nahi. Aapka shukriya. Namaste.",
    "Kya aap do BHK ya teen BHK mein interested hain?",
  ] : [
    "One moment.",
    "Got it.",
    "Sure.",
    "Thank you for your time. Goodbye.",
    "Are you looking for a two BHK or three BHK?",
  ];
  for (const phrase of phrases) {
    try {
      const audio = await synthesizeSpeech(session, phrase);
      if (audio) session._ttsCache[phrase.toLowerCase().trim()] = audio;
    } catch { /* non-fatal */ }
  }
  console.log(`[tts-cache] warmed ${Object.keys(session._ttsCache).length} phrases callSid=${session.callSid}`);
}

// Wrap synthesizeSpeech to hit cache first
const _origSynthesize = synthesizeSpeech;
async function synthesizeSpeechCached(session, text) {
  const key = text.toLowerCase().trim();
  if (session._ttsCache?.[key]) {
    console.log(`[tts-cache] HIT callSid=${session.callSid}`);
    return session._ttsCache[key];
  }
  return _origSynthesize(session, text);
}

// Pre-cached filler audio per language — synthesized once, reused for entire process lifetime.
// Played immediately after STT while LLM + TTS pipeline is warming up (~1-2s gap).
const _fillerAudioCache = new Map();

async function playThinkingFiller(ws, session) {
  if (!ws || ws.readyState !== 1 || session.closed) return;
  if (session._fillerPlaying) return;
  // Snapshot outGeneration NOW — if ElevenLabs arrives before synthesis completes, it will
  // increment outGeneration and we must NOT play the filler (would kill the real audio).
  const genSnapshot = session.telephony?.outGeneration || 0;
  try {
    const lang = languageManager.getBaseLanguage(session.callSid) || "hi";
    if (!_fillerAudioCache.has(lang)) {
      const fillerText = lang === "en" ? "Sure..." : lang === "mr" ? "Haan..." : "Haan...";
      const audio = await synthesizeSpeech(session, fillerText).catch(() => null);
      if (audio) _fillerAudioCache.set(lang, audio);
    }
    const fillerAudio = _fillerAudioCache.get(lang);
    // Bail out if real audio already started (outGeneration incremented by clearEnablexMedia in eleven-stream)
    if (!fillerAudio || session.closed || ws.readyState !== 1) return;
    if ((session.telephony?.outGeneration || 0) !== genSnapshot) return;
    session._fillerPlaying = true;
    clearEnablexMedia(ws, session);
    sendEnablexMedia(ws, session, fillerAudio, "thinking-filler");
  } catch (_) {
    // Non-critical
  }
}

function remapSessionCallSid(session, nextCallSid) {
  if (!session || !nextCallSid || session.callSid === nextCallSid) return;
  const previousCallSid = session.callSid;
  const preferredLanguage = languageManager.getLanguage(previousCallSid);
  sessions.delete(previousCallSid);
  session.callSid = nextCallSid;
  sessions.set(nextCallSid, session);
  languageManager.initialize(nextCallSid, preferredLanguage);
  languageManager.clear(previousCallSid);
}

function muLawDecodeSample(muLawByte) {
  const MULAW_BIAS = 0x84;
  muLawByte = ~muLawByte & 0xff;
  const sign = muLawByte & 0x80;
  const exponent = (muLawByte >> 4) & 0x07;
  const mantissa = muLawByte & 0x0f;
  let sample = ((mantissa << 3) + MULAW_BIAS) << exponent;
  sample -= MULAW_BIAS;
  return sign ? -sample : sample;
}

function decodeMuLawToPcm16(muLawBuffer) {
  const pcm = Buffer.alloc(muLawBuffer.length * 2);
  for (let i = 0; i < muLawBuffer.length; i += 1) {
    pcm.writeInt16LE(muLawDecodeSample(muLawBuffer[i]), i * 2);
  }
  return pcm;
}

function upsamplePcm16To16k(pcm8kBuffer) {
  const sampleCount = Math.floor(pcm8kBuffer.length / 2);
  const pcm16k = Buffer.alloc(sampleCount * 4);
  for (let i = 0; i < sampleCount; i += 1) {
    const sample = pcm8kBuffer.readInt16LE(i * 2);
    pcm16k.writeInt16LE(sample, i * 4);
    pcm16k.writeInt16LE(sample, i * 4 + 2);
  }
  return pcm16k;
}

function parseWavToPcm16(wavBuffer) {
  return parseWavInfo(wavBuffer).pcm;
}

function parseWavInfo(wavBuffer) {
  const dataIndex = wavBuffer.indexOf(Buffer.from("data"));
  if (dataIndex === -1 || wavBuffer.length < 44) {
    return { pcm: wavBuffer, sampleRate: 16000 };
  }
  const dataLength = wavBuffer.readUInt32LE(dataIndex + 4);
  const sampleRate = wavBuffer.readUInt32LE(24) || 16000;
  return {
    pcm: wavBuffer.subarray(dataIndex + 8, dataIndex + 8 + dataLength),
    sampleRate,
  };
}

function resamplePcm16(pcmBuffer, fromRate, toRate) {
  if (!pcmBuffer?.length || fromRate === toRate) return pcmBuffer;
  const inputSamples = Math.floor(pcmBuffer.length / 2);
  const outputSamples = Math.max(1, Math.floor((inputSamples * toRate) / fromRate));
  const out = Buffer.alloc(outputSamples * 2);

  // When downsampling, apply a simple FIR low-pass anti-aliasing filter before
  // decimation. Without this, frequencies above toRate/2 alias back into the
  // audio band and produce crackling (e.g. 24kHz → 8kHz without filtering).
  if (fromRate > toRate) {
    const ratio = fromRate / toRate;
    // FIR window size: covers ~ratio samples either side for smooth rolloff
    const halfWin = Math.ceil(ratio);
    for (let i = 0; i < outputSamples; i++) {
      const center = (i * fromRate) / toRate;
      let sum = 0, weight = 0;
      for (let j = -halfWin; j <= halfWin; j++) {
        const idx = Math.round(center) + j;
        if (idx < 0 || idx >= inputSamples) continue;
        // Triangular window weight — simple, zero-artifact rolloff
        const w = 1 - Math.abs(j) / (halfWin + 1);
        sum += pcmBuffer.readInt16LE(idx * 2) * w;
        weight += w;
      }
      out.writeInt16LE(Math.round(sum / weight), i * 2);
    }
    return out;
  }

  // Upsampling: linear interpolation is fine (no aliasing risk)
  for (let i = 0; i < outputSamples; i += 1) {
    const sourceIndex = (i * fromRate) / toRate;
    const low = Math.floor(sourceIndex);
    const high = Math.min(low + 1, inputSamples - 1);
    const ratio = sourceIndex - low;
    const a = pcmBuffer.readInt16LE(low * 2);
    const b = pcmBuffer.readInt16LE(high * 2);
    out.writeInt16LE(Math.round(a + (b - a) * ratio), i * 2);
  }
  return out;
}

function downsamplePcm16To8k(pcm16kBuffer) {
  const inputSamples = Math.floor(pcm16kBuffer.length / 2);
  const outputSamples = Math.floor(inputSamples / 2);
  const out = Buffer.alloc(outputSamples * 2);
  for (let i = 0; i < outputSamples; i += 1) {
    const sample = pcm16kBuffer.readInt16LE(i * 4);
    out.writeInt16LE(sample, i * 2);
  }
  return out;
}

function muLawEncodeSample(sample) {
  const MULAW_MAX = 32635;
  const MULAW_BIAS = 0x84;
  let sign = 0;
  if (sample < 0) {
    sign = 0x80;
    sample = -sample;
  }
  sample = Math.min(sample, MULAW_MAX);
  sample += MULAW_BIAS;
  let exponent = 7;
  for (let expMask = 0x4000; (sample & expMask) === 0 && exponent > 0; exponent -= 1, expMask >>= 1) {}
  const mantissa = (sample >> Math.max(exponent + 3, 0)) & 0x0f;
  return (~(sign | (exponent << 4) | mantissa)) & 0xff;
}

function encodePcm16ToMuLaw(pcm8kBuffer) {
  const sampleCount = Math.floor(pcm8kBuffer.length / 2);
  const out = Buffer.alloc(sampleCount);
  for (let i = 0; i < sampleCount; i += 1) {
    out[i] = muLawEncodeSample(pcm8kBuffer.readInt16LE(i * 2));
  }
  return out;
}

function toEnablexMuLawChunks(ttsWavBuffer) {
  const { pcm, sampleRate } = parseWavInfo(ttsWavBuffer);
  // Always use the anti-aliased FIR resampler — downsamplePcm16To8k did naive 2:1
  // decimation with no low-pass filter, aliasing high frequencies back into the
  // audio band as crackle/static ("radio" sound). resamplePcm16 filters first.
  const pcm8k = resamplePcm16(pcm, sampleRate, 8000);
  const muLaw = encodePcm16ToMuLaw(pcm8k);
  const chunks = [];
  for (let offset = 0; offset < muLaw.length; offset += 160) {
    chunks.push(muLaw.subarray(offset, offset + 160));
  }
  return chunks;
}

function decodeEnablexInboundMedia(event) {
  const payload = Buffer.from(event.media.payload, "base64");
  const format = event.media.format || {};
  const encoding = String(format.encoding || "ulaw").toLowerCase();
  const sampleRate = Number(format.sample_rate || format.sampleRate || 8000);
  if (/linear|pcm|l16|s16/.test(encoding)) {
    return sampleRate === 16000 ? payload : resamplePcm16(payload, sampleRate, 16000);
  }
  const pcm = decodeMuLawToPcm16(payload);
  return sampleRate === 16000 ? pcm : resamplePcm16(pcm, sampleRate, 16000);
}

function sendEnablexMedia(ws, session, audioBuffer, label = "audio") {
  const streamId = session.telephony?.streamId;
  const voiceId = session.telephony?.voiceId || session.callSid;
  const mediaProvider = session.telephony?.provider;
  // "browser" = test harness — same media protocol over WS, browser decodes μ-law client-side
  if (!audioBuffer || ws.readyState !== WebSocket.OPEN || (mediaProvider !== "enablex" && mediaProvider !== "browser") || !streamId || !voiceId) {
    return false;
  }
  const chunks = toEnablexMuLawChunks(audioBuffer);
  // Each chunk = 160 bytes = 20ms of 8kHz ulaw audio (8000 samples/sec × 1 byte/sample × 0.02s).
  // * 40 was written when chunks were 320 bytes — now chunks are 160 bytes, so * 20 is correct.
  const playbackMs = chunks.length * 20;
  const generation = (session.telephony.outGeneration || 0) + 1;
  session.telephony.outGeneration = generation;
  session.telephony.agentSpeakingUntil    = Date.now() + playbackMs + 200;
  // Cap at 5s — covers typical greeting (3–5s) and most mid-call replies fully.
  // Previous 2.5s cap let phone-echo trigger false barge-in for 3–18s responses.
  // Genuine >5s responses rely on EnableX hardware echo cancellation beyond this.
  session.telephony.echoSuppressionUntil  = Date.now() + Math.min(playbackMs + 200, 5000);
  // Opening greeting protection — cap at 9s max (opening audio is ≤8.8s after word cap fix).
  // Old code: no cap → 16s audio → user blocked for 17s → 1011 Deepgram close.
  if (label && label.startsWith("opening-greeting")) {
    // Cap at 3.5s — greeting audio is ~4-5s but we want user to be able to say
    // "haan" after ~3s of hearing the intro rather than waiting for the full audio.
    session.telephony.openingProtectionUntil = Date.now() + Math.min(playbackMs, 3500);
  }
  if (session.inboundAudio && !session.inboundAudio.processing) {
    session.inboundAudio.chunks = [];
    session.inboundAudio.speechFrames = 0;
    session.inboundAudio.silenceFrames = 0;
  }
  console.log(`[enablex-media] sending ${label} for ${voiceId} (${audioBuffer.length} bytes, ${chunks.length} chunks)`);
  session.telephony.lastPlaybackMs = playbackMs;
  chunks.forEach((chunk, index) => {
    setTimeout(() => {
      if (ws.readyState !== WebSocket.OPEN || session.telephony.outGeneration !== generation) return;
      const seq = session.telephony.outSeq || 0;
      ws.send(
        JSON.stringify({
          event: "media",
          voice_id: voiceId,
          stream_id: streamId,
          media: {
            seq,
            timestamp: Date.now(),
            format: {
              encoding: "ulaw",
              sample_rate: 8000,
              channels: 1,
            },
            payload: chunk.toString("base64"),
          },
        })
      );
      session.telephony.outSeq = seq + 1;
    }, index * 20);
  });
  return true;
}

function scheduleAgentSideHangup(ws, session, reason = "completed-reply") {
  if (!session || session.closed || session.telephony?.hangupScheduled) {
    return;
  }
  const delayMs = Math.max(1500, (session.telephony?.lastPlaybackMs || 0) + 1200);
  const callSidSnapshot = session.callSid;
  session.telephony = {
    ...(session.telephony || {}),
    hangupScheduled: true,
    hangupReason: reason,
  };
  setTimeout(async () => {
    // Look up by snapshot callSid — session may have already been deleted from map if caller hung up first
    const current = sessions.get(callSidSnapshot) || (session.closed ? null : session);
    if (!current || current.closed) {
      return;
    }
    const voiceId = current.telephony?.voiceId || current.callSid;
    console.log("[enablex-media] agent-side hangup firing", { callSid: callSidSnapshot, voiceId, reason, delayMs });

    // Step 1: Cancel any remaining agent audio
    if (ws.readyState === WebSocket.OPEN) {
      clearEnablexMedia(ws, current);
    }

    // Step 2: Close WebSocket from our side — primary signal to EnableX to end media/call
    if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
      ws.close(1000, "agent-ended");
    }

    // Step 3: Brief pause then call REST hangup API as belt-and-suspenders.
    // Browser test sessions have no EnableX call to hang up — WS close is enough.
    if (current.telephony?.provider === "enablex") {
      await new Promise((r) => setTimeout(r, 600));
      try {
        await hangupEnablexCall(voiceId);
        console.log("[enablex-media] hangup API succeeded", { callSid: callSidSnapshot, voiceId });
      } catch (error) {
        console.warn("[enablex-media] hangup API call failed", {
          callSid: callSidSnapshot,
          voiceId,
          status: error.response?.status,
          body: error.response?.data || error.message,
        });
      }
    }

    // Step 4: Clean up our session (ws.on("close") may also call endCall, but endCall is idempotent)
    await endCall(current, "agent_completed");
  }, delayMs);
}

function clearEnablexMedia(ws, session) {
  if (ws.readyState !== WebSocket.OPEN || !session?.telephony?.streamId) return;
  session.telephony.outGeneration = (session.telephony.outGeneration || 0) + 1;
  ws.send(
    JSON.stringify({
      event: "clear_media",
      voice_id: session.telephony.voiceId || session.callSid,
      stream_id: session.telephony.streamId,
    })
  );
}

// ── Streaming mulaw queue ─────────────────────────────────────────────────────
// Accepts raw G.711 μ-law bytes at 8kHz from ElevenLabs (ulaw_8000) and sends
// to EnableX in 160-byte chunks at 40ms intervals. No conversion needed.
function createMulawStreamQueue(ws, session, label = "stream") {
  const voiceId  = session.telephony?.voiceId  || session.callSid;
  const streamId = session.telephony?.streamId;
  if (!streamId || !voiceId || ws.readyState !== WebSocket.OPEN) return null;
  const queueProvider = session.telephony?.provider;
  if (queueProvider !== "enablex" && queueProvider !== "browser") return null;

  const generation = (session.telephony.outGeneration || 0) + 1;
  session.telephony.outGeneration = generation;
  session.telephony.agentSpeakingUntil   = Date.now() + 30000; // tentative — updated on close()
  session.telephony.echoSuppressionUntil = Date.now() + 5000;  // 5s cap — updated on close() with actual duration
  if (session.inboundAudio) { session.inboundAudio.chunks = []; session.inboundAudio.speechFrames = 0; }

  const queue   = [];
  let totalSent = 0;
  let running   = false;
  let isClosed  = false;
  let leftover  = Buffer.alloc(0); // partial PCM16 bytes waiting for a full pair

  function stopped() {
    return session.telephony.outGeneration !== generation || ws.readyState !== WebSocket.OPEN;
  }

  function tick() {
    if (stopped() || queue.length === 0) { running = false; return; }
    const mulaw = queue.shift();
    totalSent++;
    const seq = (session.telephony.outSeq || 0) + 1;
    session.telephony.outSeq = seq;
    try {
      ws.send(JSON.stringify({
        event: "media", stream_id: streamId, voice_id: voiceId,
        media: {
          seq,
          timestamp: Date.now(),
          format: { encoding: "ulaw", sample_rate: 8000, channels: 1 },
          payload: mulaw.toString("base64"),
        },
      }));
    } catch {}
    // 20ms interval = 160 bytes / 20ms = 8000 bytes/sec = correct G.711 8kHz rate.
    // 40ms was half the bitrate → buffer underruns at EnableX → crackling audio.
    setTimeout(tick, 20);
  }

  function kickSender() {
    if (!running && queue.length > 0 && !stopped()) { running = true; tick(); }
  }

  console.log(`[mulaw-queue] open label=${label} callSid=${session.callSid}`);

  return {
    // ElevenLabs sends raw ulaw_8000 bytes (G.711 μ-law, 8kHz, 1 byte/sample).
    // No conversion needed — split into 160-byte chunks (20ms each at 8kHz ulaw) and queue.
    // Chunks are sent every 20ms → EnableX receives at correct telephony bitrate (8000 bytes/sec).
    appendUlaw(ulawBytes) {
      if (isClosed || stopped()) return;
      const buf  = Buffer.concat([leftover, ulawBytes]);
      const step = 160; // 160 bytes = 160 samples = 20ms of 8kHz ulaw per chunk
      let   i    = 0;
      for (; i + step <= buf.length; i += step) {
        queue.push(buf.slice(i, i + step));
      }
      leftover = buf.slice(i);
      kickSender();
    },

    close() {
      // Flush any remaining partial ulaw bytes (pad to 160)
      if (leftover.length > 0) {
        const padded = Buffer.concat([leftover, Buffer.alloc(160 - (leftover.length % 160), 0xff)]);
        queue.push(padded);
        leftover = Buffer.alloc(0);
        kickSender();
      }
      isClosed = true;
      // Each chunk = 160 bytes = 20ms at 8kHz ulaw. * 20 gives the true audio duration.
      // +200ms margins on both agentSpeakingUntil / echoSuppressionUntil — minimal tail.
      const pendingMs = (totalSent + queue.length) * 20;
      session.telephony.lastPlaybackMs       = pendingMs;
      session.telephony.agentSpeakingUntil   = Date.now() + pendingMs + 200;
      session.telephony.echoSuppressionUntil = Date.now() + Math.min(pendingMs + 200, 5000); // cap at 5s
      console.log(`[mulaw-queue] closed totalSent=${totalSent} pending=${queue.length} playbackMs=${pendingMs} callSid=${session.callSid}`);
    },

    isStopped() { return stopped(); },
  };
}

// ── True streaming pipeline: LLM tokens → ElevenLabs WS → mulaw queue ────────
// TTFA: ~500-800ms  vs  5-6s with sequential HTTP pipeline.
// Falls back to standard pipeline on any error (caller detects null return).
//
// Returns: reply string when done   |   null when caller should use standard pipeline
async function streamingLLMWithElevenLabs(ws, session, userText, { onFirstAudio } = {}) {
  const elevenKey = process.env.ELEVENLABS_API_KEY;
  const openaiKey = process.env.OPENAI_API_KEY;
  const ttsProvider = (session.providerOverrides?.tts || process.env.TTS_PROVIDER || "elevenlabs").toLowerCase();

  // LLM selection — per-session override > LLM_PREFER_GROQ env > OpenAI default.
  // Groq's and Gemini's chat/completions endpoints are both OpenAI-compatible,
  // so the exact same SSE parser below works for all three providers unchanged.
  const llmPref  = session.providerOverrides?.llm
    || (process.env.LLM_PREFER_GROQ === "true" ? "groq" : "openai");
  const useGroq   = llmPref === "groq"   && !!process.env.GROQ_API_KEY;
  const useGemini = llmPref === "gemini" && !!process.env.GEMINI_API_KEY;
  const llmUrl   = useGemini ? "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
    : useGroq ? "https://api.groq.com/openai/v1/chat/completions" : "https://api.openai.com/v1/chat/completions";
  const llmKey   = useGemini ? process.env.GEMINI_API_KEY : useGroq ? process.env.GROQ_API_KEY : openaiKey;
  const llmModel = useGemini ? (process.env.GEMINI_MODEL || "gemini-2.5-flash")
    : useGroq ? (process.env.GROQ_MODEL || "llama-3.3-70b-versatile")
    : (process.env.OPENAI_MODEL || "gpt-4.1");
  if (!elevenKey || !llmKey || ttsProvider !== "elevenlabs") return null;

  const callSid  = session.callSid;
  // Hard cap for ElevenLabs streaming — Hindi TTS is ~1.4 words/sec, 15 words ≈ 10s audio.
  // agentConfig.wordCap may be much larger (e.g. 55 set in dashboard); we apply the
  // minimum of the two so the system prompt and the audio cap agree.
  const agentWordCap = parseInt(session.agentConfig?.wordCap || "99", 10);
  // Safety-net cap only — max_tokens=70 already bounds the LLM to ~14-16 words.
  // Set high (30) so a normal reply is NEVER hard-cut mid-word; this only catches
  // a true runaway. The LLM finishes its sentence naturally well before 30.
  const maxWords = Math.min(agentWordCap, parseInt(process.env.TTS_MAX_WORDS_STREAM || "30", 10));
  // ttsModel override only applies here when it's an ElevenLabs model id (browser test may carry a Sarvam model)
  const modelOverride = /^eleven/.test(session.providerOverrides?.ttsModel || "") ? session.providerOverrides.ttsModel : null;
  const model    = modelOverride || process.env.ELEVENLABS_MODEL || "eleven_multilingual_v2";

  // Voice ID — per-session override > env by gender
  const gender = session.campaign?.voice_gender || session.lead?.voice_gender || "female";
  const voiceId = session.providerOverrides?.ttsVoice || (gender === "male"
    ? (process.env.ELEVENLABS_VOICE_MALE   || "pNInz6obpgDQGcFmaJgB")
    : (process.env.ELEVENLABS_VOICE_FEMALE || process.env.ELEVENLABS_VOICE_ID || "1qEiC6qsybMkmnNdVMbK"));

  // Emotion → voice settings
  const emotion = emotionFromContext(userText, { stage: session.stage });
  // stability near 1.0 = ElevenLabs' "flat/monotone" mode — it actively SUPPRESSES
  // expressiveness. Previous values (0.88-0.95) made every emotion sound nearly
  // identical and robotic regardless of which one was picked. Real conversational
  // range lives around 0.3-0.55 stability with meaningful style separation between
  // emotions. speed kept close to 1.0 — the old 0.82-0.90 made the voice drawl.
  const ESETTINGS = {
    warm:         { stability: 0.40, similarity_boost: 1.0, style: 0.45, speed: 1.02 },
    excited:      { stability: 0.28, similarity_boost: 1.0, style: 0.65, speed: 1.08 },
    empathetic:   { stability: 0.55, similarity_boost: 1.0, style: 0.30, speed: 0.95 },
    professional: { stability: 0.50, similarity_boost: 1.0, style: 0.25, speed: 1.00 },
    neutral:      { stability: 0.45, similarity_boost: 1.0, style: 0.35, speed: 1.00 },
  };
  const voiceSettings = ESETTINGS[emotion] || ESETTINGS.neutral;

  // Build LLM messages — mirror getLLMResponse logic exactly
  const language = languageManager.getLanguage(callSid);
  // Push user turn to history (same as getLLMResponse line 1059)
  session.history.push({ role: "user", content: userText });
  session.history = session.history.slice(-16);
  const knowledgeContext = (
    session.dynamicVariables?.knowledge_base ||
    (await getKnowledgeContext(session.campaign?.project_id || session.lead.project_id, userText).catch(() => ""))
  ).slice(0, 3500);
  // session._lockedLanguage takes precedence — it's set before this function is called
  // when the user explicitly says "marathi mein bolo" etc., so the CURRENT turn already
  // uses the new language rather than waiting for the next utterance.
  const resolvedLanguage = session._lockedLanguage
    || ((language === "auto" || language === "auto-IN" || !language)
      ? (languageManager.getBaseLanguage(callSid) || "hi")
      : language);
  const systemPrompt = buildSystemPrompt(session.lead, knowledgeContext, resolvedLanguage, session.agentConfig || {}, session.qualification || {});

  // Web search — fires in parallel-ish (await here, but KB fetch already finished above)
  const webSearchContext = await searchWebContext(userText, session.lead, knowledgeContext);

  const historyContext = session.history.slice(-16).slice(0, -1);
  const searchBlock = webSearchContext
    ? { role: "system", content: `WEB SEARCH RESULTS for "${userText}" (use this to answer accurately, then pivot to our project):\n${webSearchContext}` }
    : null;
  const messages = [
    { role: "system", content: systemPrompt },
    ...historyContext,
    ...(searchBlock ? [searchBlock] : []),
    { role: "user",   content: `[CURRENT — respond to this only]: ${userText}` },
  ];

  const t0 = Date.now();
  let fullText    = "";
  let wordCount   = 0;
  let doneSending = false;
  let mulawQueue  = null;
  let audioFired  = false;

  function fireOnFirstAudio() {
    if (!audioFired) { audioFired = true; if (onFirstAudio) onFirstAudio(); }
  }

  // ── Leading-filler strip, applied before ANY text reaches ElevenLabs ───────
  // By the time the WS "close" event fires the reply has already been spoken —
  // stripping after the fact can't un-speak it. So the very first piece of text
  // is held back (a few tokens' worth) until there's enough of it to tell whether
  // it's a filler opener; only then does anything get sent to ElevenLabs. This
  // adds at most a handful of tokens' worth of latency (tens of ms), not the
  // whole reply.
  let leadDecided = false;
  let leadBuffer  = "";
  function resolveLead(pieceSoFar, force) {
    const hasBreak = /[\s.,…]$/.test(pieceSoFar);
    if (!force && pieceSoFar.length < 15 && !hasBreak) return null; // keep buffering
    leadDecided = true;
    const strippedLead = stripFillerOpener(pieceSoFar);
    if (strippedLead !== pieceSoFar) {
      console.log(`[eleven-stream] stripped filler opener callSid=${callSid}`);
      fullText  = strippedLead + fullText.slice(pieceSoFar.length);
      wordCount = fullText.trim().split(/\s+/).length;
    }
    return strippedLead;
  }

  return new Promise((resolve, reject) => {
    const wsUrl =
      `wss://api.elevenlabs.io/v1/text-to-speech/${voiceId}/stream-input` +
      // ulaw_8000 = G.711 μ-law at 8kHz — directly compatible with EnableX, no conversion needed.
      // optimize_streaming_latency is only supported on turbo/flash models — v3 ignores or rejects it.
      `?model_id=${model}&output_format=ulaw_8000` +
      (model.includes("v3") ? "" : "&optimize_streaming_latency=1");
    let elevenWs;
    try { elevenWs = new WebSocket(wsUrl, { headers: { "xi-api-key": elevenKey } }); }
    catch (e) { return reject(e); }

    elevenWs.on("open", async () => {
      // BOS — voice settings sent before any text
      elevenWs.send(JSON.stringify({
        text: " ",
        voice_settings: voiceSettings,
        // chunk_length_schedule: controls audio chunk sizes ElevenLabs sends back.
        // [50, 100...] was too aggressive — small chunks on phone calls (ulaw_8000) cause
        // robotic/glitchy audio because the G.711 codec needs sufficient audio length to
        // maintain natural prosody. Use larger chunks for clean phone call quality.
        generation_config: { chunk_length_schedule: [80, 120, 200] },
      }));

      // LLM streaming — tokens pipe directly into ElevenLabs WS
      try {
        const llmResp = await axios.post(
          llmUrl,
          { model: llmModel, messages, temperature: 0.4, max_tokens: 70, stream: true },
          { headers: { Authorization: `Bearer ${llmKey}` }, responseType: "stream", timeout: 8000 }
        );
        let remainder = "";
        llmResp.data.on("data", (chunk) => {
          if (doneSending) return;
          const text  = remainder + chunk.toString("utf8");
          const lines = text.split("\n");
          remainder   = lines.pop() || "";
          let batch   = "";
          for (const line of lines) {
            if (!line.startsWith("data: ")) continue;
            const d = line.slice(6).trim();
            if (d === "[DONE]") {
              doneSending = true;
              if (batch) {
                if (!leadDecided) {
                  leadBuffer += batch;
                  const resolved = resolveLead(leadBuffer, /* force */ true);
                  if (resolved) elevenWs.send(JSON.stringify({ text: normalizeTtsText(resolved) }));
                } else {
                  elevenWs.send(JSON.stringify({ text: normalizeTtsText(batch) }));
                }
              }
              elevenWs.send(JSON.stringify({ text: "" }));
              return;
            }
            try { const tok = JSON.parse(d).choices?.[0]?.delta?.content || ""; fullText += tok; batch += tok; wordCount = fullText.trim().split(/\s+/).length; } catch {}
          }
          if (!batch || doneSending) return;

          if (!leadDecided) {
            leadBuffer += batch;
            const resolved = resolveLead(leadBuffer, false);
            if (resolved === null) return; // still buffering — not enough text yet to decide
            batch = resolved; // send the (possibly filler-stripped) buffered lead now
          }

          // Normalize abbreviations BEFORE sending to ElevenLabs — fixes "BHKA", "2BHK" mispronunciations
          const normalizedBatch = normalizeTtsText(batch);
          if (wordCount >= maxWords) {
            doneSending = true;
            elevenWs.send(JSON.stringify({ text: normalizedBatch }));
            elevenWs.send(JSON.stringify({ text: "" }));   // word cap — EOS
          } else {
            elevenWs.send(JSON.stringify({ text: normalizedBatch, try_trigger_generation: wordCount >= 5 }));
          }
        });
        llmResp.data.on("end", () => { if (!doneSending) { doneSending = true; elevenWs.send(JSON.stringify({ text: "" })); } });
        llmResp.data.on("error", (e) => { console.warn(`[eleven-stream] llm err callSid=${callSid}: ${e.message}`); elevenWs.close(); });
      } catch (e) { console.warn(`[eleven-stream] llm start err callSid=${callSid}: ${e.message}`); elevenWs.close(); reject(e); }
    });

    elevenWs.on("message", (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        if (msg.audio) {
          // ElevenLabs sends ulaw_8000: raw G.711 μ-law bytes, ready for EnableX
          const ulaw = Buffer.from(msg.audio, "base64");
          if (!mulawQueue) {
            // CRITICAL ORDER: clearEnablexMedia FIRST (increments outGeneration to N+1, sends
            // clear_media to EnableX), THEN createMulawStreamQueue (increments to N+2, captures N+2).
            // Reversed order causes stopped()=true immediately → totalSent=0 (no audio plays).
            clearEnablexMedia(ws, session);
            mulawQueue = createMulawStreamQueue(ws, session, "eleven-stream");
            console.log(`[eleven-stream] TTFA=${Date.now() - t0}ms callSid=${callSid}`);
            fireOnFirstAudio();
          }
          if (mulawQueue && !mulawQueue.isStopped()) mulawQueue.appendUlaw(ulaw);
        }
        if (msg.isFinal) elevenWs.close();
      } catch {}
    });

    elevenWs.on("close", async () => {
      if (mulawQueue) mulawQueue.close();
      fireOnFirstAudio(); // ensure lock released even if no audio arrived
      const reply = fullText.trim();
      const clean = reply.replace(/OUTCOME:({.*})/s, "").trim();
      // History and OUTCOME are managed by caller (getLLMResponse already pushed user msg)
      // Push assistant reply here since we bypassed getLLMResponse
      session.history.push({ role: "assistant", content: reply });
      const m = reply.match(/OUTCOME:({.*})/s);
      if (m) { try { session.outcome = JSON.parse(m[1]); } catch {} }
      console.log(`[eleven-stream] done TTFA=${Date.now()-t0}ms reply="${clean.slice(0,60)}" callSid=${callSid} audioFired=${audioFired}`);

      // ── BUG FIX: silent turns ────────────────────────────────────────────────
      // The WS can close normally (isFinal / server close) after the LLM produced
      // text but BEFORE any `msg.audio` ever arrived — e.g. ElevenLabs returned an
      // empty/errored generation without a WS-level error event. Previously this
      // resolved as a "success" with reply text, so the caller never fell back —
      // the client would see the transcript logged but hear nothing. Now: if we
      // have text but never streamed audio, synthesize it via the REST fallback
      // right here so the turn is never silently dropped.
      if (clean && !audioFired && ws.readyState === WebSocket.OPEN && !session.closed) {
        console.warn(`[eleven-stream] no audio arrived despite text reply — REST fallback callSid=${callSid}`);
        try {
          const fallbackAudio = await synthesizeSpeech(session, clean);
          if (fallbackAudio && ws.readyState === WebSocket.OPEN && !session.closed) {
            clearEnablexMedia(ws, session);
            await recordAgentAudio(session, fallbackAudio, "agent-reply-fallback");
            sendEnablexMedia(ws, session, fallbackAudio, "eleven-stream-silent-fallback");
          }
        } catch (err) {
          console.warn(`[eleven-stream] REST fallback also failed callSid=${callSid}: ${err.message}`);
        }
      }

      // Broadcast to live feed dashboard subscribers
      if (clean) broadcastLiveEvent(session, { type: "agent_reply", text: clean });
      resolve(clean);
    });

    elevenWs.on("error", (e) => {
      console.warn(`[eleven-stream] ws error callSid=${callSid}: ${e.message}`);
      fireOnFirstAudio();
      reject(e);
    });
  });
}

// ── Streaming LLM → local TTS pipeline ───────────────────────────────────────
// Reduces TTFA on the non-ElevenLabs path from ~3-5s to ~1-1.5s by overlapping
// LLM token generation with TTS synthesis per sentence:
//
//  Before: [  full LLM reply: 1-3s  ] → [ TTS full reply: 1-2s ] → play
//  After:  [ tokens → sentence 1 ] → [ TTS s1: 600ms ] → play s1
//                                    [ tokens → sentence 2 ] → [ TTS s2: 600ms ] → play s2
//
// Returns: full reply string on success  |  null → caller falls back to getLLMResponse
async function streamingLLMWithLocalTTS(ws, session, userText) {
  // Only used when ElevenLabs is not the TTS provider (per-session override aware)
  const ttsProvider = (session.providerOverrides?.tts || process.env.TTS_PROVIDER || "elevenlabs").toLowerCase();
  if (ttsProvider === "elevenlabs" && process.env.ELEVENLABS_API_KEY) return null;

  // LLM selection — same override pattern as streamingLLMWithElevenLabs. All three
  // providers expose an OpenAI-compatible chat/completions endpoint, so the SSE
  // parser below is shared unchanged.
  const llmPref   = session.providerOverrides?.llm
    || (process.env.LLM_PREFER_GROQ === "true" ? "groq" : "openai");
  const useGroq   = llmPref === "groq"   && !!process.env.GROQ_API_KEY;
  const useGemini = llmPref === "gemini" && !!process.env.GEMINI_API_KEY;
  const llmUrl    = useGemini ? "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
    : useGroq ? "https://api.groq.com/openai/v1/chat/completions" : "https://api.openai.com/v1/chat/completions";
  const llmKey    = useGemini ? process.env.GEMINI_API_KEY : useGroq ? process.env.GROQ_API_KEY : process.env.OPENAI_API_KEY;
  const llmModel  = useGemini ? (process.env.GEMINI_MODEL || "gemini-2.5-flash")
    : useGroq ? (process.env.GROQ_MODEL || "llama-3.3-70b-versatile")
    : (process.env.OPENAI_MODEL || "gpt-4.1");
  if (!llmKey) return null;

  const callSid = session.callSid;
  const maxWords = parseInt(process.env.TTS_MAX_WORDS || "18", 10);

  // Build messages — mirrors getLLMResponse logic (knowledge context + web search)
  const language = languageManager.getLanguage(callSid);
  session.history.push({ role: "user", content: userText });
  session.history = session.history.slice(-16);
  const knowledgeContext = (
    session.dynamicVariables?.knowledge_base ||
    (await getKnowledgeContext(session.campaign?.project_id || session.lead.project_id, userText).catch(() => ""))
  ).slice(0, 3500);
  const resolvedLanguage = session._lockedLanguage
    || ((language === "auto" || language === "auto-IN" || !language)
      ? (languageManager.getBaseLanguage(callSid) || "hi")
      : language);
  const systemPrompt = buildSystemPrompt(session.lead, knowledgeContext, resolvedLanguage, session.agentConfig || {}, session.qualification || {});
  const webSearchContext = await searchWebContext(userText, session.lead, knowledgeContext).catch(() => "");
  const historyContext = session.history.slice(-16).slice(0, -1);
  const searchBlock = webSearchContext
    ? { role: "system", content: `WEB SEARCH RESULTS for "${userText}" (use this to answer accurately, then pivot to our project):\n${webSearchContext}` }
    : null;
  const messages = [
    { role: "system", content: systemPrompt },
    ...historyContext,
    ...(searchBlock ? [searchBlock] : []),
    { role: "user", content: `[CURRENT — respond to this only]: ${userText}` },
  ];

  const t0 = Date.now();
  let fullText    = "";
  let tokenBuf    = "";
  let wordCount   = 0;
  let sentCount   = 0;
  const MAX_SENTS = 3;

  // In-order playback: each TTS call pushes a Promise onto this chain.
  // Sentences synthesize in parallel but play in arrival order.
  let drainChain  = Promise.resolve();
  let firstSent   = false;
  const generation = (session.telephony?.outGeneration || 0);

  function stopped() {
    return session.closed || ws.readyState !== WebSocket.OPEN
      || (session.telephony?.outGeneration || 0) !== generation;
  }

  // Fire TTS for one sentence fragment, chain playback in order
  function fireSentence(text) {
    if (sentCount >= MAX_SENTS || stopped()) return;
    sentCount++;
    const idx = sentCount;
    // Strip a filler opener ("Haan...", "Achha...") on the FIRST sentence only —
    // same fix as the ElevenLabs streaming path, applied here since this path knows
    // the full sentence text upfront (no token-buffering needed).
    if (idx === 1) text = stripFillerOpener(text);
    const ttsPromise = synthesizeSpeech(session, normalizeTtsText(text)).catch(() => null);
    drainChain = drainChain.then(async () => {
      if (stopped()) return;
      const audio = await ttsPromise;
      if (!audio || stopped()) return;
      if (!firstSent) {
        clearEnablexMedia(ws, session);
        firstSent = true;
      }
      await recordAgentAudio(session, audio, "agent-reply");
      sendEnablexMedia(ws, session, audio, `stream-local-${idx}`);
      const playMs = session.telephony?.lastPlaybackMs || 800;
      await new Promise(r => setTimeout(r, Math.min(playMs + 80, 4000)));
    });
  }

  // Sentence boundary detector: fires when buffer ends with a sentence terminator
  // and has accumulated enough words to be worth synthesizing
  const SENT_RE = /[.!?।…]\s*$/;
  function flushIfSentence(final = false) {
    const clean = tokenBuf.trim();
    if (!clean) return;
    const words = clean.split(/\s+/).length;
    if (final || (SENT_RE.test(clean) && words >= 5)) {
      const capped = capReplyWords(clean, maxWords - fullText.trim().split(/\s+/).length + words);
      if (capped) fireSentence(capped);
      tokenBuf = "";
    }
  }

  try {
    const llmResp = await axios.post(
      llmUrl,
      { model: llmModel, messages, temperature: 0.4, max_tokens: 70, stream: true },
      { headers: { Authorization: `Bearer ${llmKey}` }, responseType: "stream", timeout: 8000 }
    );

    await new Promise((resolve, reject) => {
      let remainder = "";
      let totalWords = 0;
      llmResp.data.on("data", (chunk) => {
        if (sentCount >= MAX_SENTS) return;
        const text  = remainder + chunk.toString("utf8");
        const lines = text.split("\n");
        remainder   = lines.pop() || "";
        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          const d = line.slice(6).trim();
          if (d === "[DONE]") break;
          try {
            const tok = JSON.parse(d).choices?.[0]?.delta?.content || "";
            if (!tok) continue;
            fullText  += tok;
            tokenBuf  += tok;
            totalWords = fullText.trim().split(/\s+/).length;
            if (totalWords >= maxWords) {
              flushIfSentence(true);
              return;
            }
            flushIfSentence();
          } catch {}
        }
      });
      llmResp.data.on("end",   () => { flushIfSentence(true); resolve(); });
      llmResp.data.on("error", reject);
    });

    // Wait for all queued TTS + playback to finish
    await drainChain;

    if (!firstSent) {
      // LLM produced text but TTS failed — roll back history so getLLMResponse repushes cleanly
      session.history = session.history.filter(h => !(h.role === "user" && h.content === userText));
      return null;
    }

    // Keep history/transcript in sync with what was actually spoken — the first
    // fired sentence had its filler opener stripped before synthesis.
    const reply = stripFillerOpener(fullText.trim());
    session.history.push({ role: "assistant", content: reply });
    const m = reply.match(/OUTCOME:({.*})/s);
    if (m) { try { session.outcome = JSON.parse(m[1]); } catch {} }
    const clean = reply.replace(/OUTCOME:({.*})/s, "").trim();
    if (clean) broadcastLiveEvent(session, { type: "agent_reply", text: clean });
    console.log(`[local-stream] done ttfa=${Date.now()-t0}ms reply="${clean.slice(0,60)}" callSid=${callSid}`);
    return clean;

  } catch (err) {
    console.warn(`[local-stream] failed callSid=${callSid}: ${err.message}`);
    // Roll back the user message we pushed — getLLMResponse will push it again
    session.history = session.history.filter(h => !(h.role === "user" && h.content === userText));
    return null;
  }
}

async function processCallerUtterance(ws, session, callSid, reason = "utterance") {
  const inbound = session.inboundAudio;
  if (!inbound || inbound.processing || !inbound.chunks.length || session.telephony?.hangupScheduled) return;
  inbound.processing = true;
  const utteranceAudio = Buffer.concat(inbound.chunks);
  inbound.chunks = [];
  inbound.speechFrames = 0;
  inbound.silenceFrames = 0;
  inbound.lastFlushAt = Date.now();

  // 3200 bytes = 200ms of audio — catches short acks like "haan", "ji", "ok" (was 8000 = 500ms)
  const MIN_UTTERANCE_BYTES = 3200;
  if (utteranceAudio.length < MIN_UTTERANCE_BYTES) {
    console.log(`[enablex-media] skipping short utterance callSid=${callSid} bytes=${utteranceAudio.length}`);
    inbound.processing = false;
    return;
  }

  try {
    const t0 = Date.now();
    console.log(`[enablex-media] processing utterance callSid=${callSid} reason=${reason} bytes=${utteranceAudio.length}`);

    // ── STT: use speculative result if available, otherwise fire fresh ────────
    // Speculative path: promise was fired 160ms+ ago and may already be resolved.
    // If the speculative audio was shorter (we collected more after firing),
    // check if the extra audio changes things — if > 30% more bytes, re-transcribe.
    let transcription;
    const specPromise = inbound.speculativePromise;
    const specBytes   = inbound.speculativeAudio?.length || 0;
    const extraRatio  = specBytes > 0 ? utteranceAudio.length / specBytes : 2;
    inbound.speculativePromise = null;
    inbound.speculativeAudio   = null;

    if (specPromise && extraRatio < 5.0) {
      // Audio grew less than 5× since speculative fired — speculative result is close enough
      transcription = await specPromise;
      if (!transcription?.text) {
        // Speculative failed, run full transcription now
        transcription = await transcribeAudioDirect(utteranceAudio, languageManager.getBaseLanguage(callSid) || "auto", session.providerOverrides?.stt);
      }
      console.log(`[stt] SPECULATIVE callSid=${callSid} wait=${Date.now()-t0}ms text="${transcription?.text || ""}"`);
    } else {
      // Utterance grew significantly after speculative fired — full audio is more accurate
      const baseLang = languageManager.getBaseLanguage(callSid) || "auto";
      transcription = await transcribeAudioDirect(utteranceAudio, baseLang, session.providerOverrides?.stt);
      console.log(`[stt] FRESH callSid=${callSid} latency=${Date.now()-t0}ms text="${transcription?.text || ""}"`);
    }
    console.log(`[stt] result: "${transcription?.text || ""}" lang=${transcription?.language || ""} elapsed=${Date.now()-t0}ms`);
    if (!transcription.text) return;

    // Fallback STT minimum word count: Deepgram handles VAD properly so 1-word responses
    // are fine there. Local STT fallback is noisier — short fragments are often agent echo
    // or background noise. Require ≥2 words UNLESS it's a known valid 1-word response.
    const wordCount = transcription.text.trim().split(/\s+/).filter(w => w.length > 0).length;
    if (wordCount < 1) {
      console.log(`[enablex-media] skipping empty transcription callSid=${callSid}`);
      return;
    }
    // Allow common single-word responses through — both conversational words AND real-estate
    // keywords. "इन्वेस्टमेंट।" / "बजट।" / "2BHK।" are valid answers to agent questions.
    // "हैलो" / "हेलो" are both common spellings of "hello" in Hindi.
    const VALID_ONE_WORD = /^(haan|ha|ji|nahi|nahin|theek|ok|okay|yes|no|done|bilkul|zaroor|sure|accha|achha|acha|bye|hello|हाँ|हां|जी|नहीं|नहि|ठीक|ओके|बिल्कुल|ज़रूर|अच्छा|हेलो|हैलो|नमस्ते|namaste|oh|ओह|वाह|wah|waah|हम्म|hmm|हाँ|अरे|arre|oho|ओहो|sahi|सही|perfect|परफेक्ट|badhiya|बढ़िया|shandar|शानदार|investment|invest|इन्वेस्टमेंट|budget|बजट|project|प्रोजेक्ट|location|लोकेशन|bhk|2bhk|3bhk|1bhk|4bhk|price|प्राइस|visit|विजिट|interested|interest|possession|पोज़ेशन|rera|amenities|loan|emi|flat|apartment|floor|parking|garden|pool|balcony|view|classic|prime|luxury|affordable|connectivity|kab|कब|kitna|kitni|कितना|कितनी|कहाँ|kahan|kaun|कौन|kya|क्या|kyun|क्यों|dekhna|देखना|batao|बताओ|batayein|बताइए|samjhao|समझाओ|chahiye|चाहिए|lena|लेना|invest|weekend|weekday|saturday|sunday|morning|evening|hoy|होय|naahi|नाही|mala|मला|chan|छान|baro|बरो)$/i;
    if (wordCount === 1 && !VALID_ONE_WORD.test(transcription.text.trim().replace(/[।!?.…,]/g, ""))) {
      console.log(`[enablex-media] skipping 1-word noise callSid=${callSid} text="${transcription.text}"`);
      return;
    }
    // Single-character noise filter (not a real word)
    if (wordCount === 1 && transcription.text.trim().length <= 1) {
      console.log(`[enablex-media] skipping single-char noise callSid=${callSid} text="${transcription.text}"`);
      return;
    }
    // Background noise filter — ElevenLabs wraps noise transcripts in parentheses e.g. "(background music)"
    // Also filters Devanagari parenthetical stage directions from TV/radio: "(दृश्य बदल जाता है)"
    // Drop these so they don't trigger LLM responses
    const cleanText = transcription.text.trim();
    const startsWithParen = /^\(.*\)/.test(cleanText) || /^\[.*\]/.test(cleanText);
    if (startsWithParen) {
      // If the whole text is a parenthetical, skip entirely
      if (/^\(.*\)$/.test(cleanText) || /^\[.*\]$/.test(cleanText)) {
        console.log(`[enablex-media] skipping noise transcript callSid=${callSid} text="${cleanText}"`);
        return;
      }
      // If it starts with a parenthetical stage direction followed by real speech, strip the stage dir
      const stripped = cleanText.replace(/^\(.*?\)\s*/g, "").replace(/^\[.*?\]\s*/g, "").trim();
      if (!stripped) {
        console.log(`[enablex-media] skipping noise-only transcript callSid=${callSid} text="${cleanText}"`);
        return;
      }
    }

    // First-utterance TV/radio filter — before the lead has said anything meaningful,
    // long sentences with no greeting or question are almost certainly background audio
    // (TV, call-waiting music, ambient noise), not the lead speaking to us.
    if (!session.firstValidUtterance) {
      const wordCount2 = cleanText.split(/\s+/).filter(w => w.length > 0).length;
      const looksConversational = /\b(hello|haan|ha\b|hi\b|ji\b|namaste|theek|kaun|kya|bolo|nahi|nahin|bol|sun|suno|aap|tum|main|acha|accha|ok|haan ji|ha ji|kal|aaj|tell|what|how|where|when|price|cost|yes|no|sure|wait|who|why|want|know|about)\b/i.test(cleanText)
        || cleanText.includes("?") || wordCount2 <= 6;
      if (!looksConversational) {
        console.log(`[enablex-media] skipping first-utterance background noise callSid=${callSid} text="${cleanText.slice(0, 60)}"`);
        return;
      }
    }
    session.firstValidUtterance = true;

    const prevLang = languageManager.getBaseLanguage(callSid);

    // Language filter — same as Deepgram path: block unsupported languages.
    // ElevenLabs STT may return correct lang code, or may misreport (e.g. Odia text with lang=hin).
    // We filter at the language code level here; the system prompt handles script-level fallback.
    // "mar" is an alternate ISO code for Marathi returned by ElevenLabs — normalize to "mr".
    // Only allow languages the pipeline actually handles. Regional Indian languages (bn/gu/kn/ml/ta/te/pa)
    // are regularly returned by Sarvam STT when it mishears Hindi/Marathi phonetics — accepting them
    // causes the system prompt to instruct the LLM to respond in a language ElevenLabs will mispronounce.
    const SUPPORTED_STT_LANGS = new Set(["hi", "hin", "mr", "mar", "en", "hinglish", "auto"]);
    const rawSttLang = transcription.language || prevLang || "hi";
    const sttLang = rawSttLang === "mar" ? "mr" : rawSttLang;  // normalize mar → mr
    const safeLang = SUPPORTED_STT_LANGS.has(sttLang) ? sttLang : (prevLang || "hi");
    if (!SUPPORTED_STT_LANGS.has(sttLang)) {
      console.log(`[lang-detect] ignoring unsupported stt_lang="${sttLang}" keeping="${prevLang}" callSid=${callSid}`);
    }

    // Language lock — if user explicitly set a language this session, require 3+ turns
    // in a new language before switching. Prevents "यही ठीक है" flipping out of Marathi.
    const lockedLang = session._lockedLanguage;
    const langToRecord = lockedLang || safeLang;
    languageManager.recordUtterance(callSid, langToRecord, transcription.text);

    // Detect explicit language switch requests — lock the new language
    // IMPORTANT: Only trigger on clear REQUEST to switch, not on questions about the language.
    // "क्या आप मराठी लिखते हैं?" is a QUESTION, not a switch request — must NOT lock.
    // Valid switch requests: "marathi mein bolo", "marathi madhye bola", "marathi bol", standalone "marathi"
    const lcText = transcription.text.toLowerCase();
    const marathiSwitchRequest =
      /marathi\s*(mein|me|madhye|bol(?:o|iye?)?|baat|switch|chalte|karo|karte)\b/i.test(lcText) ||
      /मराठी\s*(मध्ये|में|बोल(?:िए|ो)?|मे\s*बोलो|चालते|करो|करते)/i.test(lcText) ||
      /^(marathi|मराठी|मराटी)\s*[.!]?\s*$/.test(transcription.text.trim()); // standalone word only
    if (marathiSwitchRequest) {
      session._lockedLanguage = "mr";
      console.log(`[lang-lock] locked to Marathi (explicit) callSid=${callSid}`);
    } else if (/hindi|हिंदी|हिन्दी/.test(lcText)) {
      session._lockedLanguage = "hi";
      session._marathiCount = 0;
      console.log(`[lang-lock] locked to Hindi (explicit) callSid=${callSid}`);
    } else if (/english|अंग्रेज़ी/.test(lcText)) {
      session._lockedLanguage = "en";
      session._marathiCount = 0;
      console.log(`[lang-lock] locked to English (explicit) callSid=${callSid}`);
    } else if (!session._lockedLanguage || session._lockedLanguage === "hi") {
      // Auto-detect Marathi from natural speech — user never said "marathi mein bolo"
      // but consistently uses Marathi-only markers. These words do NOT appear in Hindi.
      const marathiOnlyRx = /\b(आहे|नाही|काय|कसे|कसं|मला|तुम्हाला|आम्ही|सांगा|नक्की|छान|होय|किंमत|ताबा|जागा|बघा|सांगतो|सांगते|आहेत|नाहीत|कुठे|केव्हा|किती|चांगलं|चांगले|बरं|हवं|हवे|द्या|घ्या|पाहिजे|पाहुया|बोलूया|करूया|वाटतं|वाटते|येतं|येते|जातं|जाते|राहतं|राहते|मिळतं|मिळते|महिना|वर्ष|कोटी|लाख|रुपये)\b/.test(transcription.text);
      if (marathiOnlyRx) {
        session._marathiCount = (session._marathiCount || 0) + 1;
        console.log(`[lang-auto] Marathi markers count=${session._marathiCount} callSid=${callSid}`);
        if (session._marathiCount >= 2) {
          session._lockedLanguage = "mr";
          console.log(`[lang-lock] auto-locked to Marathi (natural speech) callSid=${callSid}`);
        }
      } else {
        // Reset counter if no Marathi markers — prevents single-utterance false positives
        if ((session._marathiCount || 0) > 0) session._marathiCount = 0;
      }
    }

    const newLang = languageManager.getBaseLanguage(callSid);
    if (prevLang !== newLang) {
      console.log(`[lang-detect] language switched ${prevLang} → ${newLang} callSid=${callSid}`);
    }
    // Extract qualification facts from this utterance — persists across entire call
    extractQualification(cleanText, session);
    if (session.qualification && Object.values(session.qualification).some(Boolean)) {
      console.log(`[qualification] callSid=${callSid}`, JSON.stringify(session.qualification));
    }

    session.stage = "qualification";
    // Upgrade status so dashboard shows call is active (not stuck at stream_started)
    if (session.status === "stream_started") session.status = "active";

    // Broadcast to live feed dashboard subscribers
    broadcastLiveEvent(session, { type: "caller_transcript", text: cleanText, language: sttLang });

    // ── Goodbye detection — intercept before LLM/streaming, close call immediately ──
    const lcCleanEn = cleanText.toLowerCase().replace(/[।!?.]/g, "").trim();
    const wordCountEn = lcCleanEn.split(/\s+/).filter(w => w.length > 0).length;
    const isGoodbyeEn =
      /^(bye|goodbye|alvida|shukriya|dhanyawaad|dhanyavaad|tata|ok bye|theek hai bye|chalte hain|chal theek|chhodo|nahi chahiye|nahin chahiye|band karo|khatam|no thanks|no thank you|not interested|abhi nahi|nahi abhi)\b/i.test(lcCleanEn) ||
      (/\b(bye|goodbye|dhanyawaad|shukriya|alvida)\b/i.test(lcCleanEn) && wordCountEn <= 5) ||
      // Devanagari/Marathi goodbye forms — "गुड बाय", "बाय बाय", "निरोप", "धन्यवाद बाय"
      /गुड\s*बाय|बाय\s*बाय|ओके\s*बाय|गुडबाय|निरोप|नमस्कार\s*बाय|ठीक\s*आहे\s*बाय|बाय$/.test(cleanText) ||
      // Pure Marathi farewells
      /^(ओके|ठीक आहे|बरं|हो)\s*(बाय|निरोप|नमस्कार)/.test(cleanText);
    if (isGoodbyeEn && !isTerminalGuidedState(session)) {
      console.log(`[agent] goodbye detected (enablex path) callSid=${callSid} text="${cleanText}"`);
      const farewellLang = languageManager.getBaseLanguage(callSid) || "hi";
      const goodbyeText = (farewellLang === "hi" || farewellLang === "hinglish")
        ? "Dhanyavaad! Main aapko jald property details share karti hoon. Bahut achha laga aapase baat karke. Aapka din shubh ho! Namaste."
        : "Thank you so much! I will share the property details with you shortly. It was lovely speaking with you. Have a wonderful day! Goodbye.";
      session.guidedState = "closed";
      const goodbyeAudio = await synthesizeSpeech(session, goodbyeText).catch(() => null);
      if (goodbyeAudio && ws.readyState === WebSocket.OPEN && !session.closed) {
        clearEnablexMedia(ws, session);
        sendEnablexMedia(ws, session, goodbyeAudio, "goodbye");
      }
      scheduleAgentSideHangup(ws, session, "user-goodbye");
      return;
    }

    // Play a sub-second thinking filler immediately — covers the LLM+TTS warm-up gap.
    // The real response audio will preempt it via clearEnablexMedia when it's ready.
    session._fillerPlaying = false;
    playThinkingFiller(ws, session); // non-blocking, fire-and-forget

    // ── ElevenLabs streaming path (low-latency, TTFA ~800ms) ──────────────────
    // Pipes LLM tokens directly to ElevenLabs WS — audio starts before LLM finishes.
    // This is the same fast path used by the Deepgram pipeline.
    // Fallback to REST-per-sentence if ElevenLabs streaming is unavailable.
    const onFirstAudioCb = () => {
      session._fillerPlaying = false;
      if (session.inboundAudio) {
        session.inboundAudio.processing  = false;
        session.inboundAudio.lastFlushAt = Date.now();
      }
    };
    let elevenStreamed = await streamingLLMWithElevenLabs(ws, session, cleanText, { onFirstAudio: onFirstAudioCb })
      .catch(async (err) => {
        // On LLM timeout: play a brief filler and retry once before falling through to fallback
        if (err.message && err.message.includes("timeout") && !session.closed && ws.readyState === WebSocket.OPEN) {
          console.warn(`[eleven-stream] LLM timeout — playing filler and retrying callSid=${callSid}`);
          const fillerLang = languageManager.getBaseLanguage(callSid) || "hi";
          const fillerText = (fillerLang === "mr") ? "एक क्षण थांबा..." : "Ek second...";
          const fillerAudio = await synthesizeSpeech(session, fillerText).catch(() => null);
          if (fillerAudio && ws.readyState === WebSocket.OPEN) {
            clearEnablexMedia(ws, session);
            sendEnablexMedia(ws, session, fillerAudio, "llm-timeout-filler");
            await new Promise(r => setTimeout(r, 800));
          }
          return streamingLLMWithElevenLabs(ws, session, cleanText, { onFirstAudio: onFirstAudioCb })
            .catch(() => null);
        }
        return null;
      });
    if (elevenStreamed !== null) {
      console.log(`[agent] streaming callSid=${callSid} total=${Date.now()-t0}ms reply="${(elevenStreamed||"").slice(0,60)}"`);
      // Check terminal state first — streaming path bypasses guided-reply, so LLM may have
      // produced a farewell without setting guidedState. Check the reply text itself.
      const streamedLower = (elevenStreamed || "").toLowerCase();
      // Only treat as farewell if the matched word appears at the END of the response (last 35 chars).
      // This prevents "Shukriya Abhishek ji!" at the START of a response from falsely closing the call.
      // "Shukriya/dhanyawaad" as openers are polite acknowledgements, not goodbyes.
      const streamedTail = streamedLower.slice(-35);
      const streamedIsFarewell = /\b(goodbye|good bye|alvida|dhanyawaad|shukriya|thank you for your time|have a (great|lovely|nice|good) day|aapka din shubh ho|phir milenge)\b/i.test(streamedTail);
      if (streamedIsFarewell && !isTerminalGuidedState(session)) {
        session.guidedState = "closed";
      }
      if (isTerminalGuidedState(session)) {
        console.log(`[agent] terminal state (streaming) scheduling hangup callSid=${callSid} state=${session.guidedState}`);
        scheduleAgentSideHangup(ws, session, session.guidedState);
        return;
      }
      // Schedule silence nudge (mirrors processTranscriptDirect behaviour)
      const nudgeLang = languageManager.getBaseLanguage(callSid) || "hi";
      // EnableX closes the media stream after ~20-22s of inactivity from either side.
      // Nudge must arrive BEFORE that threshold to keep the stream alive.
      // Default 15s — well under the inactivity threshold, short enough to feel attentive.
      const nudgeDelay = parseInt(process.env.SILENCE_NUDGE_MS || "15000", 10);
      const scheduleNudge = () => {
        // Do NOT add echo suppression offset — echo window is already expired long before nudge fires.
        const delay = nudgeDelay;
        const turnToken = session._lastTurnAt;
        setTimeout(() => {
          if (session._lastTurnAt !== turnToken || session.closed || session.telephony?.hangupScheduled) return;
          session.nudgesSent = (session.nudgesSent || 0) + 1;
          const MAX_NUDGES = parseInt(process.env.MAX_SILENCE_NUDGES || "3", 10);
          if (session.nudgesSent > MAX_NUDGES) {
            const byeText = nudgeLang === "hi" || nudgeLang === "hinglish"
              ? "Main baad mein call karti hoon. Dhanyawaad! Namaste."
              : "I'll call you back later. Thank you. Goodbye.";
            synthesizeSpeech(session, byeText).then(audio => {
              if (audio && ws.readyState === 1) sendEnablexMedia(ws, session, audio, "nudge-bye");
            });
            scheduleAgentSideHangup(ws, session, "silence-timeout");
            return;
          }
          const nudgeText = nudgeLang === "hi" || nudgeLang === "hinglish"
            ? (session.nudgesSent === 1 ? "Haan? Koi sawaal hai toh batayein, main hoon yahan." : "Lagta hai aap busy hain — kab call karein aapko?")
            : (session.nudgesSent === 1 ? "Are you there? Feel free to ask anything." : "You seem busy — when would be a better time to call?");
          synthesizeSpeech(session, nudgeText).then(audio => {
            if (audio && ws.readyState === 1 && !session.closed) {
              clearEnablexMedia(ws, session);
              sendEnablexMedia(ws, session, audio, "nudge");
            }
          });
        }, delay);
      };
      session._lastTurnAt = Date.now();
      setTimeout(scheduleNudge, 200);
      return;
    }

    // ── Fallback: streaming LLM → local TTS (sentence-pipelined) ─────────────
    // Try the streaming path first: LLM tokens → sentence boundaries → TTS in parallel.
    // TTFA improves from ~3-5s to ~1-1.5s. Falls back to sequential on any error.
    const t1 = Date.now();
    const localStreamed = await streamingLLMWithLocalTTS(ws, session, transcription.text)
      .catch(() => null);

    if (localStreamed !== null) {
      console.log(`[agent] local-stream callSid=${callSid} total=${Date.now()-t0}ms reply="${localStreamed.slice(0,60)}"`);
      if (session.inboundAudio) {
        session.inboundAudio.processing  = false;
        session.inboundAudio.lastFlushAt = Date.now();
      }
      if (isTerminalGuidedState(session)) {
        scheduleAgentSideHangup(ws, session, session.guidedState);
      }
      console.log(`[agent] total_latency=${Date.now()-t0}ms callSid=${callSid}`);
      await persistSession(session);
      return;
    }

    // Sequential fallback (no OpenAI key, or local-stream errored)
    const reply = await getLLMResponse(session, transcription.text);
    console.log(`[agent] callSid=${callSid} llm=${Date.now()-t1}ms total_to_llm=${Date.now()-t0}ms reply="${reply.slice(0,60)}"`);

    // Release processing lock before TTS playback waits — allows barge-in
    if (session.inboundAudio) {
      session.inboundAudio.processing  = false;
      session.inboundAudio.lastFlushAt = Date.now();
    }

    // Stream sentence-by-sentence — lead hears first word sooner
    const streamed = await synthesizeAndStreamReply(ws, session, reply);

    if (!streamed) {
      // Fallback: synthesize full reply in one shot
      const isHindi = (languageManager.getBaseLanguage(callSid) || "hi") === "hi";
      const ttsLastResort = isHindi
        ? "Ek second, main aapki baat samajh rahi hoon."
        : "One moment, I am processing your query.";
      const speech = await synthesizeSpeech(session, reply) ||
        await synthesizeSpeech(session, ttsLastResort);
      if (speech && ws.readyState === WebSocket.OPEN) {
        clearEnablexMedia(ws, session);
        await recordAgentAudio(session, speech, "agent-reply");
        sendEnablexMedia(ws, session, speech, "reply");
      }
    }

    if (isTerminalGuidedState(session)) {
      console.log(`[agent] terminal state reached, scheduling hangup callSid=${callSid} state=${session.guidedState}`);
      scheduleAgentSideHangup(ws, session, session.guidedState);
    } else {
      console.log(`[agent] continuing call callSid=${callSid} guidedState=${session.guidedState || "null"}`);
    }

    console.log(`[agent] total_latency=${Date.now()-t0}ms callSid=${callSid}`);
    await persistSession(session);
  } catch (error) {
    console.warn("[enablex-media] utterance handling failed", { callSid, message: error.message });
    const fallback = languageManager.fallback(callSid);
    const speech = await synthesizeSpeech(session, fallback);
    if (speech && ws.readyState === WebSocket.OPEN) {
      clearEnablexMedia(ws, session);
      await recordAgentAudio(session, speech, "agent-fallback");
      sendEnablexMedia(ws, session, speech, "fallback");
    }
  } finally {
    const currentInbound = session.inboundAudio;
    if (currentInbound) {
      currentInbound.processing = false;
      currentInbound.lastFlushAt = Date.now();
      const queuedBytes = currentInbound.chunks.reduce((s, c) => s + c.length, 0);
      if (queuedBytes > MIN_UTTERANCE_BYTES && ws.readyState === WebSocket.OPEN && !session.closed) {
        setImmediate(() => {
          processCallerUtterance(ws, session, callSid, "queued-after-processing").catch((error) =>
            console.warn("[enablex-media] queued utterance failed", { callSid, message: error.message })
          );
        });
      } else if (currentInbound.chunks.length) {
        // Discard tiny queued fragments — they're noise from the agent's playback period
        currentInbound.chunks = [];
        currentInbound.speechFrames = 0;
        currentInbound.silenceFrames = 0;
      }
    }
  }
}

// ── Deepgram Streaming STT ─────────────────────────────────────────────────────
// Opens a per-call WebSocket directly to Deepgram's live transcription API.
// EnableX sends μ-law 8kHz audio; Deepgram natively handles this encoding.
// With endpointing=150ms, Deepgram fires speech_final quickly when the caller
// pauses — we process it immediately without any silence-wait buffer.
// Savings vs. old pipeline: ~700ms per turn (600ms silence wait + ~100ms STT).
//
// Set DEEPGRAM_API_KEY env var to enable. Falls back to local VAD+STT if unset.
function openDeepgramStream(ws, session, callSid) {
  // Hard disable — set DEEPGRAM_ENABLED=false to use local Whisper STT instead.
  // Local STT handles Indian accents + Hinglish better than Deepgram nova-2-general.
  if (process.env.DEEPGRAM_ENABLED === "false") {
    console.log(`[deepgram] disabled via DEEPGRAM_ENABLED=false — using local STT callSid=${callSid}`);
    return null;
  }
  const dgKey = process.env.DEEPGRAM_API_KEY;
  if (!dgKey) {
    console.log(`[deepgram] DEEPGRAM_API_KEY not set — using local STT pipeline callSid=${callSid}`);
    return null;
  }
  if (session.deepgramWs?.readyState === WebSocket.OPEN) return session.deepgramWs;

  const lang = languageManager.getBaseLanguage(callSid) || "hi";
  // Deepgram language strategy:
  // • "language=multi" is NOT a valid Deepgram param — causes HTTP 400.
  // • "detect_language=true" + "language=xxx" together also cause HTTP 400.
  // • For Hindi/Hinglish: use detect_language=true ONLY (no language param).
  //   Deepgram nova-2 auto-detects Hindi, English, and code-switched Hinglish correctly.
  // • For regional Indian languages: use explicit language code (mr, ta, te, etc.)
  // • Override with DEEPGRAM_LANGUAGE env var if needed (e.g. "hi" to force Hindi).
  const forcedLang = process.env.DEEPGRAM_LANGUAGE || "";
  const baseLang   = languageManager.getBaseLanguage(callSid) || "hi";
  // null → use detect_language=true (no language param) — correct for Hinglish/auto-detect
  const langMap    = { hi: null, hinglish: null, en: "en-IN", mr: "mr", ta: "ta", te: "te", kn: "kn", ml: "ml", bn: "bn", gu: "gu", pa: "pa" };
  const dgLang     = forcedLang || langMap[baseLang]; // undefined/null = use detect_language
  const dgParams = new URLSearchParams({
    encoding:        "mulaw",
    sample_rate:     "8000",
    model:           process.env.DEEPGRAM_MODEL || "nova-2-general",
    // 250ms silence → speech_final. 150ms was too aggressive for Hindi/Hinglish speech,
    // which has more mid-sentence pauses than English — it fired speech_final mid-thought
    // and truncated the tail of what the caller was saying. +100ms is a small latency
    // cost for a meaningfully lower false-cutoff rate; full semantic turn-detection
    // (waiting longer when the transcript trails off incomplete) is a larger follow-up.
    endpointing:     process.env.DEEPGRAM_ENDPOINTING || "250",
    interim_results: "false",   // skip partials — only act on finals
    smart_format:    "true",    // normalises numbers/punctuation
  });
  // Add language OR detect_language — NEVER both (causes 400)
  if (dgLang) {
    dgParams.set("language", dgLang);
  } else {
    dgParams.set("detect_language", "true"); // auto-detects Hindi/English/Hinglish per utterance
  }

  let dgWs;
  try {
    dgWs = new WebSocket(`wss://api.deepgram.com/v1/listen?${dgParams}`, {
      headers: { Authorization: `Token ${dgKey}` },
    });
  } catch (err) {
    console.warn(`[deepgram] WebSocket create failed callSid=${callSid}:`, err.message);
    return null;
  }

  dgWs.on("open", () => {
    console.log(`[deepgram] stream opened callSid=${callSid} lang=${lang}`);
    session.deepgramReady = true;
  });

  dgWs.on("message", async (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.type === "Error") {
      console.warn(`[deepgram] API error callSid=${callSid}:`, msg.description || msg.message);
      return;
    }
    if (msg.type !== "Results") return;

    const alt = msg.channel?.alternatives?.[0];
    const transcript = (alt?.transcript || "").trim();
    if (!transcript) return;

    // ONLY act on speech_final — the definitive end-of-utterance signal after silence.
    // is_final fires on committed audio chunks (can be partial mid-sentence) — skip those.
    // Without this, partial phrases like "How would you" reach the LLM and get wrong answers.
    if (!msg.speech_final) return;

    const conf = alt?.confidence || 0;
    console.log(`[deepgram] speech_final callSid=${callSid} text="${transcript.slice(0, 80)}" conf=${conf.toFixed(2)}`);

    // Confidence threshold — skip garbled/background-noise transcripts.
    // Phone calls in India have high background noise; low-conf short phrases are almost always noise.
    // Three-tier filter:
    //   • Any conf < 0.45 → reject always (language-switching hallucinations, e.g. Spanish at 0.40)
    //   • conf < 0.70 AND ≤3 words → reject  ("Media half food.", "Abi" echoes, random clicks)
    //   • conf < 0.60 AND ≤5 words → reject  (short ambiguous fragments)
    // ── Confidence filter — tuned for real Indian phone call speech ────────────
    // Deepgram's confidence for Hindi/Hinglish on mobile networks is typically 0.55–0.85.
    // We use a SLIDING scale: shorter phrases need higher confidence to pass
    // (short noise bursts are indistinguishable from real speech at low conf).
    //
    // CRITICAL LESSON: "Hi. Interested. Who?" at conf=0.67, 3 words is REAL USER SPEECH.
    // The old flat 0.70 threshold for ≤3 words was silently dropping genuine responses.
    //
    // Scale:
    //   1 word  → need 0.65+ (or a known conversational word at 0.55+)
    //   2 words → need 0.58+ (or a known conversational word at 0.50+)
    //   3 words → need 0.52+  ← "Hi. Interested. Who." at 0.67 PASSES now
    //   4 words → need 0.47+  (effectively the 0.45 floor)
    //   5+ words → 0.45 absolute floor (longer = harder to fake as noise)
    const MIN_CONF_ANY = parseFloat(process.env.DEEPGRAM_MIN_CONF || "0.45");
    const words = transcript.split(/\s+/).length;
    const KNOWN_CONV = /\b(hello|haan|ha|ji|nahi|nahin|theek|ok|okay|yes|no|done|bilkul|zaroor|sure|accha|achha|acha|bye|namaste|bol|bolo|sun|suno|kya|kaun|aap|tum|main|budget|bhk|price|location|project|visit|kab|kitna|kitni|details|info|batao|batayein|samjha|samjhaiye|interested|interest|dekhna|chahiye|chahie|karo|lena|dikhao)\b/i.test(transcript);
    const minConfForLength =
      words === 1 ? (KNOWN_CONV ? 0.48 : 0.65) :  // known words: 0.55→0.48 (barge-in may truncate)
      words === 2 ? (KNOWN_CONV ? 0.45 : 0.55) :  // "haan boliye", "interested hoon" → 0.45+
      words === 3 ? 0.50 :
      words <= 4  ? 0.47 :
      MIN_CONF_ANY; // 5+ words: absolute floor only
    if (conf < MIN_CONF_ANY || conf < minConfForLength) {
      console.log(`[deepgram] conf-filter skipped callSid=${callSid} conf=${conf.toFixed(2)} minNeeded=${minConfForLength} words=${words} text="${transcript}"`);
      return;
    }

    // Clear local audio buffer — Deepgram owns this utterance
    const inbound = session.inboundAudio;
    if (inbound) {
      inbound.chunks             = [];
      inbound.speechFrames       = 0;
      inbound.silenceFrames      = 0;
      inbound.speculativePromise = null;
      inbound.speculativeAudio   = null;
    }

    // Extract Deepgram's per-utterance language detection (only present when detect_language=true)
    const dgDetectedLang = msg.channel?.detected_language || msg.detected_language || null;
    if (dgDetectedLang) {
      console.log(`[deepgram] detected_language=${dgDetectedLang} callSid=${callSid}`);
    }
    await processTranscriptDirect(ws, session, callSid, transcript, "deepgram", dgDetectedLang);
  });

  dgWs.on("error", (err) => {
    console.warn(`[deepgram] error callSid=${callSid}:`, err.message);
    session.deepgramWs    = null;
    session.deepgramReady = false;
  });

  dgWs.on("close", (code) => {
    console.log(`[deepgram] closed callSid=${callSid} code=${code}`);
    session.deepgramWs    = null;
    session.deepgramReady = false;
    // Auto-reconnect on server-side errors (1011=internal error, 1006=abnormal close)
    // Don't reconnect if the call is already closed or we intentionally closed (1000)
    if (!session.closed && code !== 1000 && code !== 1001) {
      setTimeout(() => {
        if (!session.closed && !session.deepgramWs) {
          console.log(`[deepgram] reconnecting after code=${code} callSid=${callSid}`);
          openDeepgramStream(ws, session, callSid);
        }
      }, 1000);
    }
  });

  session.deepgramWs    = dgWs;
  session.deepgramReady = false;  // set true on "open"
  return dgWs;
}

function closeDeepgramStream(session) {
  const dgWs = session?.deepgramWs;
  if (!dgWs) return;
  session.deepgramWs    = null;
  session.deepgramReady = false;
  try {
    if (dgWs.readyState === WebSocket.OPEN) {
      dgWs.send(JSON.stringify({ type: "CloseStream" }));
    }
    dgWs.terminate();
  } catch {}
}

// processTranscriptDirect — fast path when Deepgram already produced the transcript.
// Same pipeline as processCallerUtterance but STT is skipped entirely.
async function processTranscriptDirect(ws, session, callSid, transcriptText, source = "deepgram", detectedLanguage = null) {
  const inbound = session.inboundAudio;
  if (!inbound || inbound.processing || session.telephony?.hangupScheduled || session.closed) return;

  // ── Echo suppression at transcript level ────────────────────────────────────
  // Audio is now always forwarded to Deepgram (to prevent Deepgram 1011 on long responses).
  // Echo guard moved here: drop any transcript that fires while agent is speaking or during
  // the brief echo tail — these are the agent's own voice reflecting back from the phone.
  if (session.telephony?.echoSuppressionUntil && Date.now() < session.telephony.echoSuppressionUntil) {
    if (isLikelyEcho(transcriptText, session)) {
      console.log(`[deepgram] echo-suppressed transcript="${transcriptText.slice(0, 40)}" callSid=${callSid}`);
      return;
    }
    console.log(`[deepgram] within suppression window but content doesn't match agent's last line — treating as real speech callSid=${callSid} transcript="${transcriptText.slice(0, 60)}"`);
    // fall through — this is genuine caller speech, not an echo
  }

  // Deduplicate — Deepgram can fire speech_final twice for the same phrase
  if (
    session._lastDgTranscript === transcriptText &&
    Date.now() - (session._lastDgTranscriptAt || 0) < 1500
  ) {
    console.log(`[${source}] dedup transcript, skipping callSid=${callSid}`);
    return;
  }
  session._lastDgTranscript   = transcriptText;
  session._lastDgTranscriptAt = Date.now();

  inbound.processing = true;
  const t0 = Date.now();

  try {
    console.log(`[${source}] processing transcript callSid=${callSid} text="${transcriptText.slice(0, 80)}"`);

    const cleanText = transcriptText.trim();

    // Background noise filter — parenthetical noise markers
    if (/^\(.*\)$/.test(cleanText) || /^\[.*\]$/.test(cleanText)) {
      console.log(`[${source}] noise transcript, skipping callSid=${callSid}`);
      return;
    }

    const wordCount = cleanText.split(/\s+/).filter(w => w.length > 0).length;
    if (wordCount < 1) return;
    if (wordCount === 1 && cleanText.length <= 1) return;

    // First-utterance TV/radio background noise filter
    if (!session.firstValidUtterance) {
      const looksConversational =
        /\b(hello|haan|ha\b|hi\b|ji\b|namaste|theek|kaun|kya|bolo|nahi|nahin|bol|sun|suno|aap|tum|main|acha|accha|ok|haan ji|ha ji|kal|aaj|tell|what|how|where|when|price|cost|yes|no|sure|wait|who|why|want|know|about)\b/i.test(cleanText)
        || cleanText.includes("?") || wordCount <= 6;
      if (!looksConversational) {
        console.log(`[${source}] first-utterance background noise, skipping callSid=${callSid} text="${cleanText.slice(0, 60)}"`);
        return;
      }
    }
    session.firstValidUtterance = true;

    // Reset silence-nudge counter ONLY for substantive responses (≥4 words).
    // Single-word replies ("Hello?", "Location", "Project") don't count as real engagement —
    // resetting on those caused an infinite nudge #1 loop because the counter never grew.
    const wordCountForNudgeReset = cleanText.split(/\s+/).filter(w => w.length > 0).length;
    if (wordCountForNudgeReset >= 4) {
      session.nudgesSent = 0;
    }

    // Language tracking — prefer Deepgram's detected_language over our prior guess.
    // FILTER: only accept Indian languages + English. If Deepgram detects Spanish, French,
    // Italian etc. (e.g. misheard "Apoyé" for a Hindi word), we ignore it and keep the
    // current language — the agent should never switch to a non-Indian language.
    const SUPPORTED_CALL_LANGS = new Set(["hi", "mr", "ta", "te", "pa", "bn", "gu", "kn", "ml", "en", "hinglish", "auto", "auto-IN"]);
    const prevLang = languageManager.getBaseLanguage(callSid);
    const effectiveDgLang = (detectedLanguage && SUPPORTED_CALL_LANGS.has(detectedLanguage))
      ? detectedLanguage
      : null; // Ignore unsupported language — keeps agent in current language
    if (detectedLanguage && !SUPPORTED_CALL_LANGS.has(detectedLanguage)) {
      console.log(`[lang-detect] ignoring unsupported lang="${detectedLanguage}" keeping="${prevLang}" callSid=${callSid}`);
    }
    // Language lock: respect explicit user language requests across both STT paths
    const lockedLangDg = session._lockedLanguage;
    const langForRecord = lockedLangDg || effectiveDgLang || prevLang || "hi";
    languageManager.recordUtterance(callSid, langForRecord, cleanText);

    // Detect explicit language switch requests — lock new language for this session
    const lcCleanForLang = cleanText.toLowerCase();
    if (/marathi|मराठी|मराटी/.test(lcCleanForLang)) {
      session._lockedLanguage = "mr";
      console.log(`[lang-lock] locked to Marathi (explicit,dg) callSid=${callSid}`);
    } else if (/hindi|हिंदी|हिन्दी/.test(lcCleanForLang)) {
      session._lockedLanguage = "hi";
      session._marathiCount = 0;
      console.log(`[lang-lock] locked to Hindi (explicit,dg) callSid=${callSid}`);
    } else if (/english|अंग्रेज़ी/.test(lcCleanForLang)) {
      session._lockedLanguage = "en";
      session._marathiCount = 0;
      console.log(`[lang-lock] locked to English (explicit,dg) callSid=${callSid}`);
    } else if (!session._lockedLanguage || session._lockedLanguage === "hi") {
      // Auto-detect Marathi from natural speech markers (words that never appear in Hindi)
      const marathiOnlyRx = /\b(आहे|नाही|काय|कसे|कसं|मला|तुम्हाला|आम्ही|सांगा|नक्की|छान|होय|किंमत|ताबा|जागा|बघा|सांगतो|सांगते|आहेत|नाहीत|कुठे|केव्हा|किती|चांगलं|चांगले|बरं|हवं|हवे|द्या|घ्या|पाहिजे|पाहुया|बोलूया|करूया|वाटतं|वाटते|येतं|येते|जातं|जाते|राहतं|राहते|मिळतं|मिळते|महिना|वर्ष|कोटी|लाख|रुपये)\b/.test(cleanText);
      if (marathiOnlyRx) {
        session._marathiCount = (session._marathiCount || 0) + 1;
        console.log(`[lang-auto] Marathi markers count=${session._marathiCount} (dg) callSid=${callSid}`);
        if (session._marathiCount >= 2) {
          session._lockedLanguage = "mr";
          console.log(`[lang-lock] auto-locked to Marathi (natural speech,dg) callSid=${callSid}`);
        }
      } else {
        if ((session._marathiCount || 0) > 0) session._marathiCount = 0;
      }
    }

    const newLang = languageManager.getBaseLanguage(callSid);
    if (prevLang !== newLang) {
      console.log(`[lang-detect] language switched ${prevLang} → ${newLang} (deepgram detected: ${detectedLanguage || "n/a"}) callSid=${callSid}`);
    }

    session.stage = "qualification";
    if (session.status === "stream_started") session.status = "active";

    // ── Goodbye detection — intercept before LLM, close call immediately ─────
    // If user clearly signals they want to end the call, don't fire another LLM turn.
    // Pattern: matches "bye", "thank you bye", "dhanyawaad", "alvida", etc. at start
    // OR contains "bye"/"goodbye" in a short phrase (≤5 words, indicating wrap-up).
    const lcClean = cleanText.toLowerCase().replace(/[।!?.]/g, "").trim();
    const isGoodbye =
      /^(bye|goodbye|alvida|shukriya|dhanyawaad|dhanyavaad|tata|ok bye|theek hai bye|chalte hain|chal theek|chhodo|nahi chahiye|nahin chahiye|band karo|khatam|no thanks|no thank you|not interested|abhi nahi|nahi abhi)\b/i.test(lcClean) ||
      (/\b(bye|goodbye|dhanyawaad|shukriya|alvida)\b/i.test(lcClean) && wordCountForNudgeReset <= 5);
    if (isGoodbye && !isTerminalGuidedState(session)) {
      console.log(`[agent] goodbye detected callSid=${callSid} text="${cleanText}"`);
      const lang = languageManager.getBaseLanguage(callSid) || "hi";
      const goodbyeText = (lang === "hi" || lang === "hinglish")
        ? "Dhanyavaad! Main aapko jald property details share karti hoon. Bahut achha laga aapase baat karke. Aapka din shubh ho! Namaste."
        : "Thank you so much! I will share the property details with you shortly. It was lovely speaking with you. Have a wonderful day! Goodbye.";
      session.guidedState = "closed";
      const goodbyeAudio = await synthesizeSpeech(session, goodbyeText).catch(() => null);
      if (goodbyeAudio && ws.readyState === WebSocket.OPEN && !session.closed) {
        clearEnablexMedia(ws, session);
        sendEnablexMedia(ws, session, goodbyeAudio, "goodbye");
      }
      scheduleAgentSideHangup(ws, session, "user-goodbye");
      return;
    }

    const t1 = Date.now();

    // ── Try ElevenLabs streaming pipeline first (LLM tokens → TTS → audio in ~500ms) ──
    // Falls back to sequential pipeline on any error.
    let reply = "";
    let usedStreaming = false;

    function releaseLock() {
      if (session.inboundAudio) {
        session.inboundAudio.processing  = false;
        session.inboundAudio.lastFlushAt = Date.now();
      }
    }

    try {
      const streamResult = await streamingLLMWithElevenLabs(ws, session, cleanText, {
        onFirstAudio: releaseLock,
      });
      if (streamResult !== null) {
        // Streaming handled LLM + TTS + history push — done
        reply = streamResult;
        usedStreaming = true;
        console.log(`[agent] streaming callSid=${callSid} total=${Date.now()-t0}ms reply="${reply.slice(0,60)}"`);
      }
      // streamResult === null means streaming not configured → no history push happened
    } catch (err) {
      // Streaming was attempted (history push already happened) but failed partway.
      // Roll back the user history push so getLLMResponse doesn't double-push.
      const last = session.history[session.history.length - 1];
      if (last?.role === "user" && last.content === cleanText) session.history.pop();
      console.warn(`[eleven-stream] fallback to HTTP pipeline callSid=${callSid}: ${err.message}`);
    }

    if (!usedStreaming) {
      // Standard sequential pipeline (fallback / non-ElevenLabs TTS)
      reply = await getLLMResponse(session, cleanText);
      console.log(`[agent] callSid=${callSid} llm=${Date.now()-t1}ms total=${Date.now()-t0}ms reply="${reply.slice(0,60)}"`);
      releaseLock(); // release before TTS so barge-in works during playback

      const streamed = await synthesizeAndStreamReply(ws, session, reply);
      if (!streamed) {
        const isHindiDg = (languageManager.getBaseLanguage(callSid) || "hi") === "hi";
        const ttsLastResortDg = isHindiDg
          ? "Ek second, main aapki baat samajh rahi hoon."
          : "One moment, I am processing your query.";
        const speech = await synthesizeSpeech(session, reply) ||
          await synthesizeSpeech(session, ttsLastResortDg);
        if (speech && ws.readyState === WebSocket.OPEN) {
          clearEnablexMedia(ws, session);
          await recordAgentAudio(session, speech, "agent-reply");
          sendEnablexMedia(ws, session, speech, "reply");
        }
      }
    }

    if (isTerminalGuidedState(session)) {
      console.log(`[agent] terminal state reached, scheduling hangup callSid=${callSid} state=${session.guidedState}`);
      scheduleAgentSideHangup(ws, session, session.guidedState);
    } else {
      console.log(`[agent] continuing call callSid=${callSid} guidedState=${session.guidedState || "null"}`);

      // Silence nudge — fires if lead doesn't respond after the agent finishes speaking.
      // Timer starts from echoSuppressionUntil (when user CAN actually speak), not from
      // when the agent's LLM started — otherwise the nudge fires before echo suppression
      // even ends, giving the user almost no time to respond.
      const nudgeDelay  = parseInt(process.env.SILENCE_NUDGE_MS || "15000", 10);
      // echoSuppressionUntil is updated when the streaming queue closes; grab it now with
      // a small polling delay so it reflects the final close() value.
      const scheduleNudge = () => {
        // Do NOT add echo suppression offset — echo window is already expired long before nudge fires.
        const delay = nudgeDelay;
        const turnToken = session._lastTurnAt;
        setTimeout(async () => {
          if (session.closed || session._lastTurnAt !== turnToken || !ws || ws.readyState !== 1) return;
          const nudgeLang = languageManager.getBaseLanguage(callSid) || "hi";

          // Track nudge count per call — cap at 2, then hangup gracefully.
          // After 2 unanswered nudges the lead is clearly unavailable.
          session.nudgesSent = (session.nudgesSent || 0) + 1;
          console.log(`[agent] silence-nudge #${session.nudgesSent} callSid=${callSid}`);

          const MAX_NUDGES = parseInt(process.env.MAX_SILENCE_NUDGES || "3", 10);
          if (session.nudgesSent > MAX_NUDGES) {
            // Lead not responding — say goodbye and hang up
            const byeText = nudgeLang === "hi" || nudgeLang === "hinglish"
              ? "Main baad mein call karti hoon. Dhanyawaad! Namaste."
              : "I'll try calling you at a better time. Thank you! Goodbye.";
            console.log(`[agent] nudge-limit reached, hanging up callSid=${callSid}`);
            const byeAudio = await synthesizeSpeech(session, byeText).catch(() => null);
            if (byeAudio && ws.readyState === 1 && !session.closed) {
              clearEnablexMedia(ws, session);
              sendEnablexMedia(ws, session, byeAudio, "nudge-bye");
            }
            scheduleAgentSideHangup(ws, session, "no-response");
            return;
          }

          // Vary nudge text: first nudge is a gentle check-in, second is a final prompt
          let nudgeText;
          if (nudgeLang === "hi" || nudgeLang === "hinglish") {
            nudgeText = session.nudgesSent === 1
              ? "Haan? Koi sawaal hai toh batayein, main hoon yahan."
              : "Lagta hai aap busy hain — kab call karein aapko?";
          } else {
            nudgeText = session.nudgesSent === 1
              ? "Are you there? Feel free to ask anything."
              : "You seem busy — when would be a better time to call?";
          }

          const nudgeAudio = await synthesizeSpeech(session, nudgeText).catch(() => null);
          if (nudgeAudio && ws.readyState === 1 && !session.closed) {
            clearEnablexMedia(ws, session);
            sendEnablexMedia(ws, session, nudgeAudio, "nudge");
          }
        }, delay);
      };
      session._lastTurnAt = Date.now();
      // Schedule nudge after a brief pause to let echoSuppressionUntil settle (set by queue close())
      setTimeout(scheduleNudge, 200);
    }

    console.log(`[agent] total_latency=${Date.now() - t0}ms callSid=${callSid} source=${source}`);
    await persistSession(session);

  } catch (error) {
    console.warn(`[${source}] processing failed`, { callSid, message: error.message });
    const fallback = languageManager.fallback(callSid);
    const speech = await synthesizeSpeech(session, fallback);
    if (speech && ws.readyState === WebSocket.OPEN) {
      clearEnablexMedia(ws, session);
      await recordAgentAudio(session, speech, "agent-fallback");
      sendEnablexMedia(ws, session, speech, "fallback");
    }
  } finally {
    if (session.inboundAudio) {
      session.inboundAudio.processing    = false;
      session.inboundAudio.lastFlushAt   = Date.now();
    }
  }
}

// ── SPECULATIVE_STT_FRAMES: fire STT after this many speech frames ─────────────
// Used in the LOCAL fallback pipeline (when Deepgram is not available).
// 12 frames × 20ms = 240ms of speech → fires speculative STT while still collecting.
// Was 14 (280ms); reduced to 12 to save ~40ms on short acks like "haan", "ji", "ok".
// Was 8 (160ms) originally but fired too early, cutting users mid-sentence.
const SPECULATIVE_STT_FRAMES = 12;

// ── handleCallerAudioFrame — accepts optional rawMulaw for Deepgram forwarding ─
// rawMulaw: the raw μ-law bytes from EnableX before PCM decoding (extracted by
// the WebSocket message handler so we avoid re-encoding on every frame).
async function handleCallerAudioFrame(ws, session, callSid, audioBuffer, rawMulaw = null) {
  if (!session.inboundAudio) {
    session.inboundAudio = {
      chunks: [], speechFrames: 0, silenceFrames: 0,
      bargeinFrames: 0,           // consecutive speech frames during agent playback
      bargeinBuffer: [],          // mulaw frames buffered during barge-in detection → flushed to Deepgram on confirm
      processing: false, lastFlushAt: Date.now(),
      speculativePromise: null,   // in-flight STT request fired early
      speculativeAudio: null,     // audio snapshot sent speculatively
    };
  }
  await recordCallerAudio(session, audioBuffer, "caller-media");

  // ── Agni mode: stream audio directly to LiveKit, skip local VAD/STT/LLM/TTS ──
  if (session.agniBridge?.connected) {
    session.agniBridge.pushCallerAudio(audioBuffer);
    return;
  }

  const inbound = session.inboundAudio;
  const hasSpeech = detectSpeech(audioBuffer); // sync — no HTTP, ~0.05ms

  // Opening protection — the greeting plays fully before we listen for anything.
  // This prevents background noise or an early "hello" from cutting off the opening.
  if (session.telephony?.openingProtectionUntil && Date.now() < session.telephony.openingProtectionUntil) {
    return; // Drop all inbound audio while opening plays
  }

  // ── Barge-in detection ───────────────────────────────────────────────────────
  // Caller speaks while agent is playing → cancel agent audio after sustained frames.
  // Echo suppression window: ignore barge-in for the first echoSuppressionUntil ms — the
  // phone mic picks up the agent's own TTS playback and detectSpeech returns true, causing
  // a false barge-in that kills the mulaw queue with hundreds of chunks still pending.
  // Only count real barge-in AFTER the echo window passes.
  // Threshold lowered 6→4 frames (120ms→80ms): 6 frames clipped the first syllable of a
  // deliberate interruption before the agent audio actually stopped.
  const BARGEIN_CONFIRM_FRAMES = 4;
  if (session.telephony?.agentSpeakingUntil && Date.now() < session.telephony.agentSpeakingUntil) {
    if (hasSpeech && Date.now() >= (session.telephony.echoSuppressionUntil || 0)) {
      inbound.bargeinFrames = (inbound.bargeinFrames || 0) + 1;
      if (inbound.bargeinFrames >= BARGEIN_CONFIRM_FRAMES) {
        // Barge-in confirmed — stop agent audio, clear suppression windows
        clearEnablexMedia(ws, session);
        session.telephony.agentSpeakingUntil   = 0;
        session.telephony.echoSuppressionUntil = 0;
        inbound.bargeinFrames      = 0;
        inbound.bargeinBuffer      = [];
        inbound.speculativePromise = null;
        inbound.speculativeAudio   = null;
        console.log(`[enablex-media] barge-in confirmed (${BARGEIN_CONFIRM_FRAMES} frames) callSid=${callSid}`);
      }
    } else {
      inbound.bargeinFrames = 0; // reset on silence or during echo suppression window
      inbound.bargeinBuffer = [];
    }
  } else {
    inbound.bargeinFrames = 0;
    inbound.bargeinBuffer = [];
  }

  // ── Deepgram streaming path (primary when DEEPGRAM_API_KEY is set) ───────────
  // ALWAYS send audio to Deepgram — even during agent playback and echo window.
  //
  // WHY: Deepgram closes the WS with code 1011 when it receives no audio for ~10-15s.
  // A 15-second agent TTS response previously starved Deepgram → 1011 crash → reconnect
  // → user speech lost during reconnect → nudge loop ("agent can't hear me").
  //
  // Echo suppression is now enforced at the TRANSCRIPT level in processTranscriptDirect
  // rather than the audio level. Deepgram may transcribe agent echo during playback, but
  // those transcripts are silently dropped by echoSuppressionUntil check in processTranscriptDirect.
  if (session.deepgramWs?.readyState === WebSocket.OPEN && session.deepgramReady) {
    // Anti-aliased resample (not naive decimation) — aliasing here degrades STT accuracy
    // just as much as it distorts TTS output, and shows up as missed/garbled words.
    const mulaw = rawMulaw || encodePcm16ToMuLaw(resamplePcm16(audioBuffer, 16000, 8000));
    try {
      session.deepgramWs.send(mulaw);
    } catch (err) {
      console.warn(`[deepgram] send failed callSid=${callSid}:`, err.message);
      session.deepgramWs    = null;
      session.deepgramReady = false;
      // Fall through to local pipeline below on this frame
    }
    if (session.deepgramWs) return;  // Deepgram owns this frame
  }

  // ── Local STT path: echo suppression at audio level ─────────────────────────
  // Deepgram handles echo suppression at transcript level (processTranscriptDirect).
  // Local STT has no transcript-level guard — must drop frames here to prevent
  // the agent's own voice from being sent to local VAD and re-transcribed.
  if (session.telephony?.agentSpeakingUntil && Date.now() < session.telephony.agentSpeakingUntil) {
    return;
  }
  if (session.telephony?.echoSuppressionUntil && Date.now() < session.telephony.echoSuppressionUntil) {
    return;
  }

  // ── Local VAD + silence detection (fallback when Deepgram is not available) ──
  const isCollecting = inbound.chunks.length > 0;
  if (hasSpeech || isCollecting) inbound.chunks.push(audioBuffer);
  if (inbound.processing) return;

  if (hasSpeech) {
    inbound.speechFrames += 1;
    inbound.silenceFrames = 0;

    // ── Speculative STT: fire early after 8 frames (160ms) ──────────────────
    // STT processes in parallel with remaining audio collection.
    // When silence triggers, the STT may already be done — saves ~200ms.
    if (inbound.speechFrames === SPECULATIVE_STT_FRAMES && !inbound.speculativePromise && !inbound.processing) {
      const earlySnap = Buffer.concat(inbound.chunks);
      const baseLang = languageManager.getBaseLanguage(callSid) || "auto";
      inbound.speculativeAudio   = earlySnap;
      inbound.speculativePromise = transcribeAudioDirect(earlySnap, baseLang, session.providerOverrides?.stt)
        .catch(err => {
          console.warn(`[speculative-stt] failed callSid=${callSid}:`, err.message);
          return null;
        });
      console.log(`[speculative-stt] fired at ${inbound.speechFrames} frames callSid=${callSid}`);
    }
    return;
  }

  if (!isCollecting) return;
  inbound.silenceFrames += 1;
  const bufferedMs = inbound.chunks.length * 20;
  const enoughSpeech = inbound.speechFrames >= 10 || bufferedMs >= 1500;
  const endedBySilence = inbound.silenceFrames >= 15;  // 300ms silence (was 30×20ms=600ms) — cuts wait in half
  const tooLong = bufferedMs >= 10000;

  if ((enoughSpeech && endedBySilence) || tooLong) {
    await processCallerUtterance(ws, session, callSid, endedBySilence ? "silence" : "max-buffer");
  }
}

app.get("/metrics", async (_req, res) => {
  res.set("Content-Type", registry.contentType);
  res.end(await registry.metrics());
});

app.get("/health", async (_req, res) => {
  try {
    await redis.ping();
    res.json({ status: acceptingTraffic ? "ok" : "draining", active_sessions: sessions.size });
  } catch (error) {
    res.status(500).json({ status: "error", message: error.message });
  }
});

// ── ElevenLabs voices — returns only the 2 configured voices (male + female) ─
app.get("/voices", (_req, res) => {
  const femaleId = process.env.ELEVENLABS_VOICE_FEMALE || process.env.ELEVENLABS_VOICE_ID || "1qEiC6qsybMkmnNdVMbK";
  const maleId   = process.env.ELEVENLABS_VOICE_MALE   || "pNInz6obpgDQGcFmaJgB";
  const voices = [
    {
      voice_id:    femaleId,
      name:        process.env.ELEVENLABS_VOICE_FEMALE_LABEL || "Female Voice",
      gender:      "female",
      language:    "multilingual",
      accent:      "",
      preview_url: null,
    },
    {
      voice_id:    maleId,
      name:        process.env.ELEVENLABS_VOICE_MALE_LABEL || "Male Voice",
      gender:      "male",
      language:    "multilingual",
      accent:      "",
      preview_url: null,
    },
  ];
  return res.json({ voices });
});

// ── Supported languages — dashboard uses this to populate language dropdown ──
app.get("/languages", (_req, res) => {
  res.json({
    languages: [
      { code: "multilingual", label: "Multilingual" },
      { code: "en",           label: "English" },
      { code: "hi",           label: "Hindi" },
      { code: "mr",           label: "Marathi" },
      { code: "ta",           label: "Tamil" },
      { code: "te",           label: "Telugu" },
      { code: "bn",           label: "Bengali" },
      { code: "kn",           label: "Kannada" },
      { code: "gu",           label: "Gujarati" },
      { code: "pa",           label: "Punjabi" },
      { code: "ml",           label: "Malayalam" },
    ],
  });
});

// Session status — polled by dashboard Test Call panel
app.get("/sessions", requireToken, (_req, res) => {
  const list = Array.from(sessions.values()).map((s) => ({
    call_sid: s.callSid,
    status: s.status || "active",
    state: s.guidedState || null,
    closed: s.closed,
    phone: s.lead?.phone,
    lead_name: s.lead?.name,
    language: languageManager.getLanguage(s.callSid),
    started_at: s.startedAt,
  }));
  res.json({ sessions: list, count: list.length });
});

// Active calls with live feed WebSocket URL — used by dashboard live feed panel
app.get("/active-calls", requireToken, (req, res) => {
  const wsBase = getPublicWsBaseUrl(req);
  const list = Array.from(sessions.values())
    .filter(s => !s.closed)
    .map(s => ({
      call_sid: s.callSid,
      status: s.status || "active",
      state: s.guidedState || null,
      phone: s.lead?.phone || null,
      lead_name: s.lead?.name || null,
      language: languageManager.getLanguage(s.callSid),
      started_at: s.startedAt,
      turn_count: Math.floor((s.history?.length || 0) / 2),
      live_feed_url: `${wsBase}/live/${s.callSid}`,
    }));
  res.json({ calls: list, count: list.length });
});

app.get("/sessions/:callSid", requireToken, async (req, res) => {
  const callSid = req.params.callSid;
  let session = sessions.get(callSid);

  // If not in memory (e.g. after a redeploy), fall back to Redis-persisted state.
  // This lets the dashboard show call results for recently completed calls.
  if (!session) {
    try {
      const raw = await redis.get(`session:${callSid}`);
      if (raw) session = JSON.parse(raw);
    } catch {}
  }

  if (!session) {
    return res.status(404).json({ call_sid: callSid, status: "completed", state: "not_found" });
  }
  const turnCount = Math.floor((session.history?.length || 0) / 2);
  const detectedLang = languageManager.getBaseLanguage(session.callSid || callSid);
  res.json({
    call_sid: session.callSid || callSid,
    status: session.status || "active",
    state: session.guidedState || null,
    closed: session.closed,
    phone: session.lead?.phone,
    lead_name: session.lead?.name,
    language: languageManager.getLanguage(session.callSid || callSid),
    detected_language: detectedLang,
    started_at: session.startedAt,
    turn_count: turnCount,
    kb_loaded: !!(session.dynamicVariables?.knowledge_base),
    voice_gender: session.campaign?.voice_gender || "female",
    last_agent_reply: session.history?.filter(h => h.role === "assistant").slice(-1)[0]?.content?.slice(0, 100) || null,
  });
});

app.post("/call/dial", requireToken, async (req, res) => {
  if (!acceptingTraffic) {
    return res.status(503).json({ error: "Service draining" });
  }
  const lead = req.body.lead || (req.body.phone ? { id: crypto.randomUUID(), name: "Unknown Lead", phone: req.body.phone } : null);
  if (!lead || !lead.phone) {
    return res.status(400).json({ error: "lead.phone is required" });
  }
  const session = createSession(lead, req.body.campaign || {});
  // Store KB context / dynamic variables from dashboard for Agni injection
  if (req.body.dynamic_variables && typeof req.body.dynamic_variables === 'object') {
    session.dynamicVariables = req.body.dynamic_variables;
    if (req.body.dynamic_variables.knowledge_base) {
      console.log(`[dial] KB context attached (${req.body.dynamic_variables.knowledge_base.length} chars)`);
    }
  }
  // Store agent config (pitch tone, word cap, language strictness, escalation line, agent name)
  if (req.body.agent_config && typeof req.body.agent_config === 'object') {
    session.agentConfig = req.body.agent_config;
    console.log(`[dial] agent_config: tone=${session.agentConfig.pitchTone || 'balanced'} wordCap=${session.agentConfig.wordCap || 30} lang=${session.agentConfig.langStrictness || 'pure-hindi'}`);
  }
  try {
    await persistSession(session);
    const greeting = await getOpeningMessage(session);
    // Synthesize greeting and pre-warm in background — don't block the dial response.
    // Pre-warm is delayed so it doesn't compete for TTS provider concurrency with the
    // greeting itself — firing both at once was a real cause of 429 (rate limit) bursts
    // right at call start, which also slowed down the greeting/first-turn TTS.
    synthesizeSpeech(session, greeting).then(audio => { session.pendingGreetingAudio = audio; }).catch(() => {});
    setTimeout(() => { if (!session.closed) prewarmTTSCache(session).catch(() => {}); }, 3000);
    const provider = resolveTelephonyProvider(req.body.provider);

    if (provider === "enablex") {
      const openingLine = (
        req.body.opening_line ||
        req.body.campaign?.opening_line ||
        req.body.campaign?.openingLine ||
        greeting ||
        buildEnablexOpeningLine(
          lead.name || "there",
          session.agentConfig?.agentName  || "Priya",
          session.agentConfig?.companyName || "Prophunt",
          lead.project || ""
        )
      ).trim();
      console.log(`[dial] placing EnableX call to=${lead.phone} from=${config.enablex.fromNumber} hasConfig=${hasEnablexConfig()}`);
      const enablexCall = await placeEnablexOutboundCall({ lead, session, openingLine });
      remapSessionCallSid(session, enablexCall.provider_call_id);
      session.telephony = {
        provider: "enablex",
        from: config.enablex.fromNumber,
        to: lead.phone,
        callSid: enablexCall.provider_call_id,
      };
      session.status = enablexCall.provider_status;
      scheduleEnablexStreamStart(session, "post-dial");
      await persistSession(session);
      return res.json({
        call_sid: enablexCall.provider_call_id,
        lead_id: lead.id,
        phone: lead.phone,
        status: enablexCall.provider_status,
        greeting: openingLine,
        provider: "enablex",
        provider_response: enablexCall.raw,
        kb_attached: !!(session.dynamicVariables?.knowledge_base),
        kb_chars: session.dynamicVariables?.knowledge_base?.length || 0,
      });
    }

    return res.json({
      call_sid: session.callSid,
      lead_id: lead.id,
      phone: lead.phone,
      status: "queued",
      greeting,
      provider: "simulated",
    });
  } catch (error) {
    console.error("[dial] error placing call:", error.message, error.response?.data);
    return res.status(502).json({
      error: "Failed to place outbound call",
      details: error.response?.data || error.message,
      call_sid: session.callSid,
      lead_id: lead.id,
    });
  }
});

app.post("/call/bulk-dial", requireToken, async (req, res) => {
  const campaignId = req.body.campaign_id || crypto.randomUUID();
  const leads = req.body.leads || (await fetchDialableLeads(campaignId, req.body.limit || 10, req.body.filters || {}));
  const results = [];
  for (const lead of leads.slice(0, config.maxConcurrentCalls)) {
    const session = createSession(lead, req.body.campaign || {});
    await persistSession(session);
    results.push({ call_sid: session.callSid, lead_id: lead.id, phone: lead.phone, status: "queued" });
  }
  res.json({ campaign_id: campaignId, queued: results.length, results });
});

// ── Browser test harness (Phase 0) ───────────────────────────────────────────
// Creates a call session that runs the FULL production pipeline (VAD/STT/LLM/TTS,
// barge-in, echo logic) but exchanges audio with a browser page over the media WS
// instead of EnableX. Providers are switchable per session AND mid-call, so
// STT/TTS/LLM engines can be A/B tested by ear without spending telephony minutes.
const BROWSER_TEST_PROVIDERS = {
  stt: new Set(["deepgram", "sarvam", "elevenlabs", "gemini"]),
  tts: new Set(["elevenlabs", "sarvam", "gemini"]),
  llm: new Set(["openai", "groq", "gemini"]),
};

function sanitizeProviderOverrides(input = {}) {
  const out = {};
  if (BROWSER_TEST_PROVIDERS.stt.has(input.stt)) out.stt = input.stt;
  if (BROWSER_TEST_PROVIDERS.tts.has(input.tts)) out.tts = input.tts;
  if (BROWSER_TEST_PROVIDERS.llm.has(input.llm)) out.llm = input.llm;
  if (typeof input.ttsVoice === "string" && /^[a-zA-Z0-9_-]{2,40}$/.test(input.ttsVoice.trim())) {
    out.ttsVoice = input.ttsVoice.trim();
  }
  if (typeof input.ttsModel === "string" && /^[a-z0-9_.:-]{3,40}$/i.test(input.ttsModel.trim())) {
    out.ttsModel = input.ttsModel.trim();
  }
  return out;
}

app.post("/call/browser-test", requireToken, async (req, res) => {
  const body = req.body || {};
  const lead = {
    id: `browser-test-${Date.now()}`,
    name: String(body.lead_name || "Test User").slice(0, 60),
    phone: "browser-test",
    project: String(body.project || "Test Project").slice(0, 100),
    language: body.language || "hi",
    voice_gender: body.voice_gender === "male" ? "male" : "female",
  };
  const campaign = {
    name: lead.project,
    opening_line: String(body.opening_line || "").slice(0, 500),
    voice_gender: lead.voice_gender,
  };
  const session = createSession(lead, campaign);
  session.isBrowserTest = true;
  session.providerOverrides = sanitizeProviderOverrides(body.providers || {});
  if (body.knowledge_base) {
    session.dynamicVariables = { knowledge_base: String(body.knowledge_base).slice(0, 8000) };
  }
  session.telephony = { provider: "browser", outSeq: 0 };
  await persistSession(session);
  const wsBase = getPublicWsBaseUrl(req);
  console.log(`[browser-test] session created callSid=${session.callSid} providers=${JSON.stringify(session.providerOverrides)}`);
  return res.json({
    call_sid: session.callSid,
    ws_url: `${wsBase}/?callSid=${session.callSid}`,
    live_url: `${wsBase}/live/${session.callSid}`,
    providers: session.providerOverrides,
  });
});

// Mid-call provider switch — takes effect on the agent's next turn.
// Switching STT away from Deepgram closes the stream (batch path takes over);
// switching back to Deepgram reopens it on the stored media WS.
app.post("/call/browser-test/:callSid/providers", requireToken, (req, res) => {
  const session = sessions.get(req.params.callSid);
  if (!session || !session.isBrowserTest || session.closed) {
    return res.status(404).json({ error: "Browser test session not found or ended" });
  }
  session.providerOverrides = { ...session.providerOverrides, ...sanitizeProviderOverrides(req.body || {}) };
  const stt = session.providerOverrides.stt;
  if (stt && stt !== "deepgram") {
    closeDeepgramStream(session);
  } else if (stt === "deepgram" && !session.deepgramWs && session._mediaWs?.readyState === WebSocket.OPEN) {
    openDeepgramStream(session._mediaWs, session, session.callSid);
  }
  console.log(`[browser-test] providers switched callSid=${session.callSid} providers=${JSON.stringify(session.providerOverrides)}`);
  return res.json({ ok: true, providers: session.providerOverrides });
});

app.post("/call/inbound", async (req, res) => {
  const { phone } = req.body;
  if (!phone) {
    return res.status(400).json({ error: "phone is required" });
  }
  try {
    const lead = await fetchLeadByPhone(phone);
    const session = createSession(lead, {});
    await persistSession(session);
    res.json({ call_sid: session.callSid, lead });
  } catch {
    res.status(404).json({ error: "Lead not found" });
  }
});

app.all("/call/enablex/events", async (req, res) => {
  const payload = req.body && Object.keys(req.body).length ? req.body : req.query;
  const callSid = extractEnablexCallSid(payload);
  const callStatus = normalizeEnablexStatus(payload);
  console.log("[enablex-event] received", {
    voice_id: callSid,
    status: callStatus,
    keys: Object.keys(payload || {}),
    payload,
  });
  const session = callSid ? sessions.get(callSid) : null;

  if (session) {
    session.status = callStatus || session.status;
    session.telephony = {
      ...(session.telephony || {}),
      provider: "enablex",
      lastEvent: payload,
    };
    if (shouldStartEnablexStream(callStatus)) {
      scheduleEnablexStreamStart(session, `event-${callStatus}`, { force: callStatus === "connected" });
    }
    if (["completed", "disconnected", "failed", "busy", "no-answer", "cancelled", "canceled"].includes(callStatus)) {
      clearTimeout(session.timer);
      await endCall(session, callStatus);
    } else {
      await persistSession(session);
    }
  }

  res.json({ status: "ok" });
});

wss.on("connection", (ws, req) => {
  console.log(`[enablex-media] websocket connected url=${req.url || "/"}`);
  const wsUrl = new URL(req.url, "http://localhost");
  const pathParts = wsUrl.pathname.split("/").filter(Boolean);

  // ── Live feed dashboard subscribers: /live/:callSid ─────────────────────
  if (pathParts[0] === "live") {
    const feedCallSid = pathParts[1];
    const feedSession = feedCallSid ? sessions.get(feedCallSid) : null;
    if (!feedSession) {
      ws.send(JSON.stringify({ type: "error", message: "call not found" }));
      ws.close(4004, "call not found");
      return;
    }
    if (!feedSession.liveSubscribers) feedSession.liveSubscribers = new Set();
    feedSession.liveSubscribers.add(ws);
    console.log(`[live-feed] subscriber joined callSid=${feedCallSid} total=${feedSession.liveSubscribers.size}`);

    // Send current call state immediately so the UI can show lead info
    ws.send(JSON.stringify({
      type: "call_status",
      callSid: feedCallSid,
      status: feedSession.status || "active",
      lead_name: feedSession.lead?.name || null,
      phone: feedSession.lead?.phone || null,
      language: languageManager.getLanguage(feedCallSid),
      started_at: feedSession.startedAt,
      timestamp: Date.now(),
    }));

    // Replay transcript history so new subscribers see the full conversation so far
    const history = feedSession.history || [];
    for (const item of history) {
      if (item.role === "user" && item.content !== "[CALL_STARTED]") {
        try { ws.send(JSON.stringify({ type: "caller_transcript", text: item.content, callSid: feedCallSid, timestamp: Date.now() })); } catch (_) {}
      } else if (item.role === "assistant") {
        const clean = item.content.replace(/OUTCOME:({.*})/s, "").trim();
        if (clean) try { ws.send(JSON.stringify({ type: "agent_reply", text: clean, callSid: feedCallSid, timestamp: Date.now() })); } catch (_) {}
      }
    }

    const liveHeartbeat = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.ping();
    }, 15000);

    ws.on("close", () => {
      clearInterval(liveHeartbeat);
      if (feedSession.liveSubscribers) feedSession.liveSubscribers.delete(ws);
      console.log(`[live-feed] subscriber left callSid=${feedCallSid}`);
    });
    return;
  }
  // ── End live feed ────────────────────────────────────────────────────────

  const requestedCallSid = wsUrl.searchParams.get("callSid") || pathParts[pathParts.length - 1] || crypto.randomUUID();
  let activeCallSid = requestedCallSid;
  let session = sessions.get(requestedCallSid) || null;
  const heartbeat = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) ws.ping();
  }, 10000);

  ws.on("message", async (message, isBinary) => {
    let audioBuffer = null;
    if (isBinary) {
      if (!session) return;
      console.log(`[enablex-media] binary frame received bytes=${Buffer.byteLength(message)}`);
      audioBuffer = Buffer.from(message);
    } else {
      try {
        const event = JSON.parse(message.toString());
        if (event.event !== "media") {
          console.log(`[enablex-media] event received type=${event.event || "unknown"}`);
        }
        if (event.event === "connected") {
          console.log("[enablex-media] connected");
          return;
        }
        if (event.event === "start_media") {
          const voiceId = event.start?.voice_id || event.voice_id || activeCallSid;
          const streamId = event.stream_id || event.start?.stream_id || null;
          console.log(`[enablex-media] start_media received voiceId=${voiceId} streamId=${streamId || ""}`);
          if (!session && voiceId) {
            session = sessions.get(voiceId) || null;
            activeCallSid = voiceId;
          }
          if (!session) return;
          session.telephony = {
            ...(session.telephony || {}),
            provider: session.isBrowserTest ? "browser" : "enablex",
            streamId,
            voiceId,
            callSid: voiceId,
            outSeq: session.telephony?.outSeq || 0,
          };
          session.status = "stream_started";
          session._mediaWs = ws;  // kept for mid-call STT provider switches (browser test)
          console.log(`[enablex-media] stream started for ${voiceId} provider=${session.telephony.provider}`);

          // Browser test sessions always use the local pipeline (never Agni) so that
          // STT/TTS/LLM provider overrides can be A/B tested from the test page.
          const agniActive = config.agni.enabled && !session.isBrowserTest;

          // ── Deepgram streaming STT: open per-call WebSocket for real-time transcription ──
          // Opens immediately so it's ready before the first caller utterance.
          // Falls back to local VAD+STT if DEEPGRAM_API_KEY is not set.
          // Skipped when the session's STT override picks a batch engine (sarvam/elevenlabs).
          const sttOverride = session.providerOverrides?.stt;
          if (!agniActive && (!sttOverride || sttOverride === "deepgram")) {
            openDeepgramStream(ws, session, voiceId);
          }

          // ── Agni mode: create LiveKit session, skip local greeting synthesis ──
          if (agniActive) {
            try {
              // Base vars + any KB context passed from the dashboard at dial time
              const agniDynamicVars = {
                lead_name:    session.lead?.name || "there",
                phone:        session.lead?.phone || "",
                project:      session.campaign?.name || session.campaign?.project_name || session.lead?.project || "",
                language:     session.lead?.language || session.lead?.language_preference || "english",
                opening_line: session.campaign?.opening_line || session.campaign?.openingLine || "",
                // Merge KB context + any other vars from the dashboard dial request
                ...(session.dynamicVariables || {}),
              };
              if (agniDynamicVars.knowledge_base) {
                console.log(`[agni-bridge] injecting KB context callSid=${voiceId} chars=${agniDynamicVars.knowledge_base.length}`);
              }
              const agniSession = await createAgniSession({
                apiKey:           config.agni.apiKey,
                agentId:          config.agni.agentId,
                callSid:          voiceId,
                dynamicVariables: agniDynamicVars,
              });
              console.log(`[agni-bridge] session created callSid=${voiceId} agni_session=${agniSession.session_id}`);
              session.agniSessionId = agniSession.session_id;

              const bridge = new AgniBridge({
                callSid: voiceId,
                livekitUrl: agniSession.url,
                token: agniSession.access_token,
                onAgentAudio: (pcm16Buffer) => {
                  // Agni speaks → encode μ-law → send to EnableX
                  if (ws.readyState === WebSocket.OPEN) {
                    sendEnablexMedia(ws, session, pcm16Buffer, "agni-reply");
                  }
                },
                onDisconnect: (reason) => {
                  console.log(`[agni-bridge] session ended callSid=${voiceId} reason=${reason}`);
                  // Agni hung up → clean up our side too
                  if (!session.closed) {
                    scheduleAgentSideHangup(ws, session, "agni_completed", 800);
                  }
                },
              });

              session.agniBridge = bridge;
              await bridge.connect();

              // Agni sends its own opening line — skip local TTS greeting
              session.pendingGreetingAudio = null;
              session.openingPlayedAt = nowIso();
            } catch (err) {
              console.error(`[agni-bridge] failed to start callSid=${voiceId}`, err.message);
              // Fall back to local STT/LLM/TTS pipeline
              session.agniBridge = null;
              if (!session.pendingGreetingAudio) {
                const greeting = await getOpeningMessage(session);
                session.pendingGreetingAudio = await synthesizeSpeech(session, greeting);
              }
              if (session.pendingGreetingAudio) {
                const pending = session.pendingGreetingAudio;
                setTimeout(() => {
                  if (sendEnablexMedia(ws, session, pending, "opening-greeting")) {
                    recordAgentAudio(session, pending, "opening-greeting").catch(() => {});
                    session.pendingGreetingAudio = null;
                    session.openingPlayedAt = nowIso();
                  }
                }, 700);
              }
            }
          } else {
            // ── Local pipeline mode (no Agni) ──────────────────────────────────
            if (!session.pendingGreetingAudio) {
              const greeting = await getOpeningMessage(session);
              session.pendingGreetingAudio = await synthesizeSpeech(session, greeting);
            }
            if (session.pendingGreetingAudio) {
              const pending = session.pendingGreetingAudio;
              // Fallback timer: plays opening if first-media path hasn't fired in 1200ms.
              // IMPORTANT: check openingPlayedAt — first inbound media packet plays the
              // opening immediately (see first-media handler below). Without this guard,
              // opening plays TWICE: once at ~200ms (first-media) and again at 1200ms,
              // which the caller hears as opening → 1s gap → opening again (the "4s delay").
              setTimeout(() => {
                if (session.closed) return;
                if (session.openingPlayedAt) return; // already played via first-media path
                if (sendEnablexMedia(ws, session, pending, "opening-greeting")) {
                  recordAgentAudio(session, pending, "opening-greeting").catch((error) =>
                    console.warn("[recording] opening capture failed", error.message)
                  );
                  session.pendingGreetingAudio = null;
                  session.openingPlayedAt = nowIso();
                  console.log(`[enablex-media] opening played via fallback-timer callSid=${session.callSid}`);
                  broadcastLiveEvent(session, { type: "call_status", status: "connected", lead_name: session.lead?.name, phone: session.lead?.phone });
                  const openingText = (session.history || []).find(h => h.role === "assistant")?.content;
                  if (openingText) broadcastLiveEvent(session, { type: "agent_reply", text: openingText });
                }
              }, 500);
            }
          }

          await persistSession(session);
          return;
        }
        if (event.event === "stop_media") {
          console.log(`[enablex-media] stop_media received callSid=${activeCallSid}`);
          if (!session) return;
          clearTimeout(session.timer);
          await endCall(session, "completed");
          return;
        }
        if (event.event !== "media" || !event.media?.payload) return;
        if (!session) return;
        const voiceId = event.voice_id || session.telephony?.voiceId || activeCallSid;
        const streamId = event.stream_id || session.telephony?.streamId || null;
        activeCallSid = voiceId || activeCallSid;
        session.telephony = {
          ...(session.telephony || {}),
          provider: "enablex",
          voiceId: activeCallSid,
          streamId,
          callSid: activeCallSid,
          lastInboundSeq: event.media.seq ?? session.telephony?.lastInboundSeq,
        };
        if (!session.telephony.inboundFormatLogged) {
          console.log("[enablex-media] inbound format", {
            callSid: activeCallSid,
            format: event.media.format || null,
          });
          session.telephony.inboundFormatLogged = true;
        }
        if (!session.pendingGreetingAudio && !session.openingPlayedAt) {
          const greeting = await getOpeningMessage(session);
          session.pendingGreetingAudio = await synthesizeSpeech(session, greeting);
        }
        if (session.pendingGreetingAudio && !session.openingPlayedAt) {
          const pending = session.pendingGreetingAudio;
          if (sendEnablexMedia(ws, session, pending, "opening-greeting-on-first-media")) {
            await recordAgentAudio(session, pending, "opening-greeting");
            session.pendingGreetingAudio = null;
            session.openingPlayedAt = nowIso();
            console.log(`[enablex-media] opening played via first-media callSid=${session.callSid}`);
            broadcastLiveEvent(session, { type: "call_status", status: "connected", lead_name: session.lead?.name, phone: session.lead?.phone });
            const openingText = (session.history || []).find(h => h.role === "assistant")?.content;
            if (openingText) broadcastLiveEvent(session, { type: "agent_reply", text: openingText });
          }
        }
        audioBuffer = decodeEnablexInboundMedia(event);
        // Preserve raw μ-law bytes for Deepgram (avoids re-encoding PCM→mulaw per frame).
        // Only when the inbound payload actually IS μ-law — browser test sends linear PCM,
        // which must go through the PCM→mulaw conversion in handleCallerAudioFrame instead.
        const inboundEncoding = String(event.media.format?.encoding || "ulaw").toLowerCase();
        if (session?.deepgramReady && !/linear|pcm|l16|s16/.test(inboundEncoding)) {
          session._rawMulawFrame = Buffer.from(event.media.payload, "base64");
        }
      } catch (error) {
        console.log("[enablex-media] failed to parse text frame", error.message);
        return;
      }
    }
    if (!audioBuffer) return;
    const rawMulawFrame = session?._rawMulawFrame || null;
    if (session) session._rawMulawFrame = null;
    await handleCallerAudioFrame(ws, session, activeCallSid, audioBuffer, rawMulawFrame);
  });

  ws.on("close", async () => {
    console.log(`[enablex-media] websocket closed callSid=${activeCallSid}`);
    clearInterval(heartbeat);
    if (session) {
      clearTimeout(session.timer);
      try {
        await stopEnablexStream(activeCallSid);
      } catch {}
      await endCall(session, "completed");
    }
  });
});

async function gracefulShutdown() {
  acceptingTraffic = false;
  for (const session of sessions.values()) {
    clearTimeout(session.timer);
    await endCall(session, "drained");
  }
  await redis.quit();
  server.close(() => process.exit(0));
}

process.on("SIGTERM", gracefulShutdown);
process.on("SIGINT", gracefulShutdown);
process.on("uncaughtException", (err) => console.error("[process] uncaughtException:", err.message, err.stack));
process.on("unhandledRejection", (reason) => console.error("[process] unhandledRejection:", reason));

server.listen(config.port, () => {
  console.log(`orchestrator listening on ${config.port}`);
});
