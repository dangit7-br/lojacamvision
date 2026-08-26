const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const zlib = require("zlib");
const QRCode = require("qrcode");

const root = __dirname;
const port = Number(process.env.PORT || 3000);
const dataDir = process.env.DATA_DIR || path.join(root, "data");
const dataFile = path.join(dataDir, "store.json");
const databaseUrl = process.env.DATABASE_URL || "";
const adminToken = process.env.ADMIN_TOKEN || "";
const adminUser = process.env.ADMIN_USER || "admin";
const adminPassword = process.env.ADMIN_PASSWORD || adminToken;
const onyxPublicKey = process.env.ONYXPAG_PUBLIC_KEY || "";
const onyxPrivateKey = process.env.ONYXPAG_PRIVATE_KEY || "";
const blackcatApiKey = process.env.BLACKCAT_API_KEY || process.env.BLACKCAT_PRIVATE_KEY || "";
const blackcatPublicKey = process.env.BLACKCAT_PUBLIC_KEY || "";
const blackcatSplitCode = process.env.BLACKCAT_SPLIT_CODE || "";
const utmifyApiToken = process.env.UTMIFY_API_TOKEN || "";
const onyxApiUrl = process.env.ONYXPAG_API_URL || "https://api.onyxpag.com";
const blackcatApiUrl = process.env.BLACKCAT_API_URL || "https://api.blackcatoficial.com/api";
const utmifyApiUrl = process.env.UTMIFY_API_URL || "https://api.utmify.com.br/api-credentials/orders";
const appTimezone = process.env.APP_TIMEZONE || "America/Sao_Paulo";
const defaultGateway = ["onyxpag", "blackcat"].includes(process.env.PAYMENT_GATEWAY) ? process.env.PAYMENT_GATEWAY : "onyxpag";

const MAX_BODY = 64 * 1024;
const MAX_EVENTS = Number(process.env.MAX_EVENTS || 50000);

const PRODUCT_CATALOG = {
  camvision: {
    slug: "camvision",
    id: "camera-externa-ptz-wifi-lente-dupla",
    name: "Câmera Externa PTZ WiFi Lente Dupla 2MP+2MP",
    gatewayName: "Kit camera",
    variants: {
      Kit2: { label: "Compre 1, leve 2", price: 89.9 },
      Kit4: { label: "Compre 2, leve 4", price: 139.9 },
    },
    defaultVariant: "Kit2",
  },
};

const mime = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".gif": "image/gif",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
  ".ttf": "font/ttf",
  ".mp4": "video/mp4",
};

let store = {
  startedAt: new Date().toISOString(),
  sessions: {},
  events: [],
  orders: {},
  settings: { paymentGateway: defaultGateway },
};

const clients = new Set();
const pixCreationInFlight = new Map();
let pgPool = null;
let databaseReady = false;

function normalizeStore(parsed) {
  if (!parsed || typeof parsed !== "object") return store;
  return {
    ...store,
    ...parsed,
    sessions: parsed.sessions && typeof parsed.sessions === "object" ? parsed.sessions : {},
    events: Array.isArray(parsed.events) ? parsed.events : [],
    orders: parsed.orders && typeof parsed.orders === "object" ? parsed.orders : {},
    settings: parsed.settings && typeof parsed.settings === "object" ? parsed.settings : store.settings,
  };
}

function ensureDataDir() {
  fs.mkdirSync(dataDir, { recursive: true });
}

function loadStore() {
  try {
    ensureDataDir();
    if (fs.existsSync(dataFile)) {
      const parsed = JSON.parse(fs.readFileSync(dataFile, "utf8"));
      store = normalizeStore(parsed);
    }
  } catch (error) {
    console.error("[store] failed to load:", error.message);
  }
}

async function initDatabase() {
  if (!databaseUrl) return;
  try {
    const { Pool } = require("pg");
    pgPool = new Pool({
      connectionString: databaseUrl,
      ssl: /sslmode=require/i.test(databaseUrl) ? { rejectUnauthorized: false } : false,
    });
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS app_store (
        key text PRIMARY KEY,
        value jsonb NOT NULL,
        updated_at timestamptz NOT NULL DEFAULT now()
      )
    `);
    const result = await pgPool.query("SELECT value FROM app_store WHERE key = $1", ["store"]);
    databaseReady = true;
    if (result.rows[0] && result.rows[0].value) {
      store = normalizeStore(result.rows[0].value);
      console.log("[postgres] store loaded from database");
    } else {
      await persistStoreToDatabase();
      console.log("[postgres] database initialized with current store");
    }
  } catch (error) {
    databaseReady = false;
    console.error("[postgres] failed to connect:", error.message);
    console.error("[postgres] falling back to file storage:", dataFile);
  }
}

async function persistStoreToDatabase() {
  if (!pgPool || !databaseReady) return;
  await pgPool.query(
    `INSERT INTO app_store (key, value, updated_at)
     VALUES ($1, $2::jsonb, now())
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
    ["store", JSON.stringify(store)]
  );
}

function writeStoreFile() {
  ensureDataDir();
  const tempFile = `${dataFile}.tmp`;
  fs.writeFileSync(tempFile, JSON.stringify(store, null, 2));
  fs.renameSync(tempFile, dataFile);
}

let saveTimer = null;
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    try {
      writeStoreFile();
      persistStoreToDatabase().catch((error) => console.error("[postgres] failed to save:", error.message));
    } catch (error) {
      console.error("[store] failed to save:", error.message);
    }
  }, 500);
}

function saveStoreNow() {
  if (saveTimer) {
    clearTimeout(saveTimer);
    saveTimer = null;
  }
  try {
    writeStoreFile();
    persistStoreToDatabase().catch((error) => console.error("[postgres] failed to save now:", error.message));
  } catch (error) {
    console.error("[store] failed to save now:", error.message);
  }
}

async function saveStoreNowAsync() {
  if (saveTimer) {
    clearTimeout(saveTimer);
    saveTimer = null;
  }
  writeStoreFile();
  await persistStoreToDatabase();
}

function json(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "access-control-allow-origin": "*",
  });
  res.end(body);
}

function sendDownload(res, filename, contentType, body) {
  res.writeHead(200, {
    "content-type": contentType,
    "cache-control": "no-store",
    "content-disposition": `attachment; filename="${filename}"`,
  });
  res.end(body);
}

function parseCookies(req) {
  const header = String(req.headers.cookie || "");
  return header.split(";").reduce((cookies, part) => {
    const index = part.indexOf("=");
    if (index > -1) cookies[part.slice(0, index).trim()] = decodeURIComponent(part.slice(index + 1).trim());
    return cookies;
  }, {});
}

function adminSessionValue() {
  if (!adminPassword) return "";
  return crypto.createHmac("sha256", adminPassword).update(`${adminUser}:casaorganizy-admin`).digest("hex");
}

function setAdminCookie(req, res) {
  const proto = String(req.headers["x-forwarded-proto"] || "").split(",")[0];
  const secure = proto === "https" ? "; Secure" : "";
  res.setHeader("set-cookie", `admin_auth=${encodeURIComponent(adminSessionValue())}; HttpOnly; SameSite=Lax; Path=/; Max-Age=86400${secure}`);
}

function clearAdminCookie(res) {
  res.setHeader("set-cookie", "admin_auth=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0");
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    let chunks = "";
    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY) {
        reject(new Error("Payload too large"));
        req.destroy();
        return;
      }
      chunks += chunk;
    });
    req.on("end", () => {
      if (!chunks) return resolve({});
      try {
        resolve(JSON.parse(chunks));
      } catch {
        resolve({});
      }
    });
    req.on("error", reject);
  });
}

function sanitizeText(value, max = 500) {
  if (value == null) return undefined;
  return String(value).trim().slice(0, max);
}

function digitsOnly(value, max = 32) {
  if (value == null) return undefined;
  const digits = String(value).replace(/\D+/g, "").slice(0, max);
  return digits || undefined;
}

function cents(value) {
  return Math.round((Number(value) || 0) * 100);
}

function getSettings() {
  store.settings = store.settings && typeof store.settings === "object" ? store.settings : {};
  if (!["onyxpag", "blackcat"].includes(store.settings.paymentGateway)) {
    store.settings.paymentGateway = defaultGateway;
  }
  return store.settings;
}

function getPaymentGateway() {
  return getSettings().paymentGateway;
}

function gatewayLabel(gateway) {
  return gateway === "blackcat" ? "Black Cat" : "OnyxPag";
}

function gatewayConfigured(gateway) {
  if (gateway === "blackcat") return Boolean(blackcatApiKey);
  return Boolean(onyxPublicKey && onyxPrivateKey);
}

function resolveProduct(order = {}) {
  const slug = PRODUCT_CATALOG[order.productSlug] ? order.productSlug : "camvision";
  const product = PRODUCT_CATALOG[slug];
  const requestedVariant = sanitizeText(order.variant || order.selectedKit || order.size, 40);
  const variantKey = product.variants[requestedVariant] ? requestedVariant : product.defaultVariant;
  const variant = product.variants[variantKey];
  const shippingPrice = order.shipping && typeof order.shipping.price === "number" ? order.shipping.price : 0;
  const bumpPrice = order.bump && typeof order.bump.price === "number" ? order.bump.price : 0;
  const expectedTotal = Number((variant.price + shippingPrice + bumpPrice).toFixed(2));
  const sentTotal = Number(order.total || order.value || 0);
  return {
    slug,
    id: product.id,
    name: product.name,
    gatewayName: product.gatewayName || product.name,
    variant: variantKey,
    variantLabel: variant.label,
    subtotal: variant.price,
    total: Math.abs(sentTotal - expectedTotal) <= 0.02 ? sentTotal : expectedTotal,
  };
}

function utmifyDateTime(value = new Date()) {
  const date = new Date(value);
  const safeDate = Number.isNaN(date.getTime()) ? new Date() : date;
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: appTimezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).formatToParts(safeDate).reduce((result, part) => ({ ...result, [part.type]: part.value }), {});
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
}

function publicBaseUrl(req) {
  const proto = String(req.headers["x-forwarded-proto"] || "http").split(",")[0];
  const host = req.headers["x-forwarded-host"] || req.headers.host || "localhost";
  return `${proto}://${host}`;
}

function getClientIp(req) {
  const forwarded = req.headers["x-forwarded-for"];
  if (typeof forwarded === "string" && forwarded) return forwarded.split(",")[0].trim();
  return req.socket.remoteAddress || "";
}

function cleanTracking(source = {}) {
  return {
    src: sanitizeText(source.src, 160) || null,
    sck: sanitizeText(source.sck, 160) || null,
    utm_source: sanitizeText(source.utm_source || source.utmSource, 160) || null,
    utm_campaign: sanitizeText(source.utm_campaign || source.utmCampaign, 220) || null,
    utm_medium: sanitizeText(source.utm_medium || source.utmMedium, 160) || null,
    utm_content: sanitizeText(source.utm_content || source.utmContent, 220) || null,
    utm_term: sanitizeText(source.utm_term || source.utmTerm, 220) || null,
  };
}

function normalizePayload(body, req) {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  const sessionId =
    sanitizeText(body.sessionId || body.session_id || url.searchParams.get("sessionId"), 120) ||
    crypto.randomUUID();

  const now = new Date().toISOString();
  const rawCustomer = body.customer && typeof body.customer === "object" ? body.customer : {};
  const rawShipping = body.shipping && typeof body.shipping === "object" ? body.shipping : {};
  const rawBump = body.bump && typeof body.bump === "object" ? body.bump : undefined;
  const tracking = cleanTracking(body.tracking || body);
  const customer = {
    name: sanitizeText(rawCustomer.name || body.name, 160),
    email: sanitizeText(rawCustomer.email || body.email, 220),
    phone: digitsOnly(rawCustomer.phone || body.phone, 15),
    document: digitsOnly(rawCustomer.document || rawCustomer.cpf || body.cpf, 11),
    zipcode: digitsOnly(rawCustomer.zipcode || body.cep, 8),
    street: sanitizeText(rawCustomer.street, 220),
    number: sanitizeText(rawCustomer.number, 40),
    complement: sanitizeText(rawCustomer.complement, 120),
    neighborhood: sanitizeText(rawCustomer.neighborhood, 120),
    city: sanitizeText(rawCustomer.city, 120),
    state: sanitizeText(rawCustomer.state, 2),
  };
  Object.keys(customer).forEach((key) => customer[key] === undefined && delete customer[key]);
  const shipping = Object.keys(rawShipping).length
    ? {
        id: sanitizeText(rawShipping.id, 60),
        name: sanitizeText(rawShipping.name, 120),
        price: typeof rawShipping.price === "number" ? rawShipping.price : undefined,
      }
    : undefined;
  if (shipping) Object.keys(shipping).forEach((key) => shipping[key] === undefined && delete shipping[key]);
  const bump = rawBump
    ? {
        id: sanitizeText(rawBump.id, 80),
        name: sanitizeText(rawBump.name, 160),
        price: typeof rawBump.price === "number" ? rawBump.price : undefined,
      }
    : undefined;
  if (bump) Object.keys(bump).forEach((key) => bump[key] === undefined && delete bump[key]);

  const event = {
    id: crypto.randomUUID(),
    sessionId,
    type: sanitizeText(body.type || body.kind || "activity", 80),
    stage: sanitizeText(body.stage || "browsing", 80),
    page: sanitizeText(body.page || body.path || url.pathname, 160),
    orderId: sanitizeText(body.orderId || body.order_id, 120),
    product: sanitizeText(body.product || body.size || body.kit, 80),
    value: typeof body.value === "number" ? body.value : undefined,
    subtotal: typeof body.subtotal === "number" ? body.subtotal : undefined,
    total: typeof body.total === "number" ? body.total : undefined,
    shipping,
    bump,
    src: tracking.src || undefined,
    sck: tracking.sck || undefined,
    utmSource: sanitizeText(body.utmSource || body.utm_source, 160),
    utmCampaign: sanitizeText(body.utmCampaign || body.utm_campaign, 160),
    utmMedium: sanitizeText(body.utmMedium || body.utm_medium, 160),
    utmContent: tracking.utm_content || undefined,
    utmTerm: tracking.utm_term || undefined,
    tracking,
    referrer: sanitizeText(body.referrer || req.headers.referer, 500),
    href: sanitizeText(body.href, 500),
    title: sanitizeText(body.title, 160),
    status: sanitizeText(body.status, 80),
    paymentStatus: sanitizeText(body.paymentStatus || body.payment_status, 80),
    paymentMethod: sanitizeText(body.paymentMethod || body.payment_method, 80),
    error: sanitizeText(body.error, 400),
    userAgent: sanitizeText(body.userAgent || req.headers["user-agent"], 500),
    screen: sanitizeText(body.screen, 80),
    customer: Object.keys(customer).length ? customer : undefined,
    ip: getClientIp(req),
    createdAt: now,
  };

  return { sessionId, event };
}

function summarize() {
  const sessions = Object.values(store.sessions || {});
  const now = Date.now();
  const active = sessions.filter((s) => now - new Date(s.lastSeenAt).getTime() < 60_000);
  const checkoutClicks = store.events.filter((e) => e.stage === "checkout_click").length;
  const checkoutViews = store.events.filter((e) => e.stage === "checkout_view").length;
  const identified = store.events.filter((e) => e.stage === "identification_completed").length;
  const delivered = store.events.filter((e) => e.stage === "delivery_completed").length;
  const pixFailures = store.events.filter((e) => e.stage === "pix_generation_failed").length;
  const generatedOrders = store.events.filter((e) => e.stage === "order_submitted");
  const paidOrders = store.events.filter((e) => {
    const stage = String(e.stage || "").toLowerCase();
    const status = String(e.status || e.paymentStatus || "").toLowerCase();
    return stage === "payment_paid" || stage === "paid" || status === "paid" || status === "approved";
  });
  const sumValue = (events) => events.reduce((total, e) => total + (typeof e.value === "number" ? e.value : 0), 0);
  const conversion = (part, total) => (total ? Number(((part / total) * 100).toFixed(1)) : 0);
  const generatedBySession = new Map();
  for (const event of generatedOrders) generatedBySession.set(event.sessionId, event);
  const eventKey = (event) => event.orderId || event.sessionId;
  const firstByKey = (events) => {
    const map = new Map();
    for (const event of events) {
      const key = eventKey(event);
      if (key && !map.has(key)) map.set(key, event);
      if (event.sessionId && !map.has(event.sessionId)) map.set(event.sessionId, event);
    }
    return map;
  };
  const latestByKey = (events) => {
    const map = new Map();
    for (const event of events) {
      const key = eventKey(event);
      if (key) map.set(key, event);
      if (event.sessionId) map.set(event.sessionId, event);
    }
    return map;
  };
  const pixGeneratedByKey = firstByKey(store.events.filter((e) => e.stage === "pix_generated"));
  const pixCopiedEvents = store.events.filter((e) => e.stage === "pix_code_copied");
  const pixCopiedByKey = firstByKey(pixCopiedEvents);
  const paidByKey = latestByKey(paidOrders);
  const orders = Array.from(generatedBySession.values())
    .map((event) => {
      const session = store.sessions[event.sessionId] || {};
      const savedOrder = (store.orders && (store.orders[event.orderId] || store.orders[event.id])) || {};
      const pixGeneratedEvent = pixGeneratedByKey.get(event.orderId) || pixGeneratedByKey.get(event.sessionId);
      const pixCopiedEvent = pixCopiedByKey.get(event.orderId) || pixCopiedByKey.get(event.sessionId);
      const paidEvent = paidByKey.get(event.orderId) || paidByKey.get(event.sessionId);
      const copyClicks = pixCopiedEvents.filter((copyEvent) => {
        return (event.orderId && copyEvent.orderId === event.orderId) || (event.sessionId && copyEvent.sessionId === event.sessionId);
      }).length;
      return {
        id: event.id,
        orderId: event.orderId,
        sessionId: event.sessionId,
        createdAt: event.createdAt,
        pixGeneratedAt: savedOrder.pixGeneratedAt || (pixGeneratedEvent ? pixGeneratedEvent.createdAt : event.createdAt),
        copyClickedAt: savedOrder.copyClickedAt || (pixCopiedEvent ? pixCopiedEvent.createdAt : undefined),
        copyClicks,
        paidAt: savedOrder.paidAt || (paidEvent ? paidEvent.createdAt : undefined),
        status: paidEvent || savedOrder.status === "paid" ? "paid" : "pending_payment",
        product: event.product || savedOrder.product || session.product,
        value: event.value || savedOrder.total || savedOrder.value,
        subtotal: event.subtotal || savedOrder.subtotal,
        shipping: event.shipping || savedOrder.shipping || session.shipping,
        bump: event.bump || savedOrder.bump || session.bump,
        customer: event.customer || savedOrder.customer || session.customer,
        href: event.href || savedOrder.href || session.href,
        paymentGateway: event.paymentGateway || savedOrder.paymentGateway || session.paymentGateway,
        utmify: savedOrder.utmify,
      };
    })
    .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());
  const seenOrders = new Set();
  for (const order of orders) {
    [order.orderId, order.id, order.externalOrderId, order.gatewayTransactionId].filter(Boolean).forEach((key) => seenOrders.add(String(key)));
  }
  for (const savedOrder of Object.values(store.orders || {})) {
    if (!savedOrder || typeof savedOrder !== "object") continue;
    const keys = [savedOrder.orderId, savedOrder.id, savedOrder.externalOrderId, savedOrder.gatewayTransactionId].filter(Boolean).map(String);
    if (keys.some((key) => seenOrders.has(key))) continue;
    keys.forEach((key) => seenOrders.add(key));
    const pixCopiedEvent = pixCopiedByKey.get(savedOrder.orderId) || pixCopiedByKey.get(savedOrder.externalOrderId) || pixCopiedByKey.get(savedOrder.sessionId);
    const paidEvent = paidByKey.get(savedOrder.orderId) || paidByKey.get(savedOrder.externalOrderId) || paidByKey.get(savedOrder.sessionId);
    orders.push({
      id: savedOrder.id || savedOrder.orderId,
      orderId: savedOrder.orderId || savedOrder.id,
      sessionId: savedOrder.sessionId,
      createdAt: savedOrder.createdAt || savedOrder.pixGeneratedAt || new Date().toISOString(),
      pixGeneratedAt: savedOrder.pixGeneratedAt || savedOrder.createdAt,
      copyClickedAt: savedOrder.copyClickedAt || (pixCopiedEvent ? pixCopiedEvent.createdAt : undefined),
      copyClicks: savedOrder.copyClicks || 0,
      paidAt: savedOrder.paidAt || (paidEvent ? paidEvent.createdAt : undefined),
      status: paidEvent || savedOrder.status === "paid" ? "paid" : savedOrder.status || "pending_payment",
      product: savedOrder.product,
      value: savedOrder.total || savedOrder.value,
      subtotal: savedOrder.subtotal,
      shipping: savedOrder.shipping,
      bump: savedOrder.bump,
      customer: savedOrder.customer,
      href: savedOrder.href,
      paymentGateway: savedOrder.paymentGateway,
      utmify: savedOrder.utmify,
    });
  }
  orders.sort((a, b) => new Date(b.createdAt || b.pixGeneratedAt || 0).getTime() - new Date(a.createdAt || a.pixGeneratedAt || 0).getTime());
  const paidOrderRows = orders.filter((order) => order.status === "paid");
  const revenueGenerated = orders.reduce((total, order) => total + (typeof order.value === "number" ? order.value : 0), 0);
  const revenuePaid = paidOrderRows.reduce((total, order) => total + (typeof order.value === "number" ? order.value : 0), 0);
  return {
    startedAt: store.startedAt,
    generatedAt: new Date().toISOString(),
    totals: {
      sessions: sessions.length,
      activeNow: active.length,
      events: store.events.length,
      checkoutClicks,
      checkoutViews,
      identified,
      delivered,
      pixFailures,
      generatedOrders: orders.length,
      paidOrders: paidOrderRows.length,
      pendingOrders: Math.max(0, orders.length - paidOrderRows.length),
      revenueGenerated,
      revenuePaid,
      averageTicket: orders.length ? Number((revenueGenerated / orders.length).toFixed(2)) : 0,
      conversionViewToOrder: conversion(orders.length, checkoutViews || sessions.length),
      conversionViewToPaid: conversion(paidOrderRows.length, checkoutViews || sessions.length),
      conversionIdentificationToOrder: conversion(orders.length, identified),
    },
    sessions: sessions
      .sort((a, b) => new Date(b.lastSeenAt).getTime() - new Date(a.lastSeenAt).getTime())
      .slice(0, 250),
    events: store.events.slice(-500).reverse(),
    orders: orders.slice(0, 250),
  };
}

function broadcast(payload) {
  const data = `data: ${JSON.stringify(payload)}\n\n`;
  for (const res of clients) {
    try {
      res.write(data);
    } catch {
      clients.delete(res);
    }
  }
}

function buildUtmifyPayload(order, status, paidAt) {
  const resolved = resolveProduct(order);
  const total = Number(order.total || order.value || resolved.total || 0);
  const totalCents = cents(total);
  const customer = order.customer || {};
  return {
    orderId: String(order.orderId || order.id),
    platform: "CamVision",
    paymentMethod: "pix",
    status,
    createdAt: utmifyDateTime(order.createdAt || new Date()),
    approvedDate: status === "paid" ? utmifyDateTime(paidAt || new Date()) : null,
    refundedAt: null,
    customer: {
      name: customer.name || "Cliente",
      email: customer.email || "",
      phone: customer.phone || null,
      document: customer.document || null,
      country: "BR",
      ip: order.ip || null,
    },
    products: [
      {
        id: resolved.id,
        name: resolved.gatewayName,
        planId: null,
        planName: resolved.variantLabel,
        quantity: 1,
        priceInCents: totalCents,
      },
    ],
    trackingParameters: cleanTracking(order.tracking || order),
    commission: {
      totalPriceInCents: totalCents,
      gatewayFeeInCents: 0,
      userCommissionInCents: totalCents,
      currency: "BRL",
    },
  };
}

async function sendUtmifyOrder(order, status, paidAt) {
  if (!utmifyApiToken) return { skipped: true, reason: "UTMIFY_API_TOKEN not configured" };
  const response = await fetch(utmifyApiUrl, {
    method: "POST",
    headers: { "content-type": "application/json", "x-api-token": utmifyApiToken },
    body: JSON.stringify(buildUtmifyPayload(order, status, paidAt)),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Utmify ${response.status}: ${text.slice(0, 300)}`);
  return { ok: true, status: response.status, body: text.slice(0, 500) };
}

function rememberOrder(order) {
  if (!order || !order.orderId) return;
  store.orders = store.orders || {};
  store.orders[order.orderId] = order;
  if (order.id) store.orders[order.id] = order;
  if (order.externalOrderId) store.orders[order.externalOrderId] = order;
  if (order.gatewayTransactionId) store.orders[order.gatewayTransactionId] = order;
}

async function syncUtmifyOrder(order, status, paidAt) {
  const key = status === "paid" ? "paid" : "waitingPayment";
  const sentAt = new Date().toISOString();
  try {
    const result = await sendUtmifyOrder(order, status, paidAt);
    order.utmify = {
      ...(order.utmify || {}),
      [key]: {
        ok: !result.skipped,
        skipped: Boolean(result.skipped),
        reason: result.reason,
        status: result.status,
        sentAt,
      },
    };
  } catch (error) {
    order.utmify = {
      ...(order.utmify || {}),
      [key]: {
        ok: false,
        error: error.message || "Falha ao enviar para Utmify",
        sentAt,
      },
    };
    console.error(`[utmify] ${status}:`, error.message);
  }
  rememberOrder(order);
  scheduleSave();
  broadcast({ type: "update", event: { type: "integration", stage: `utmify_${status}`, orderId: order.orderId, createdAt: new Date().toISOString() }, summary: summarize().totals });
  return order.utmify && order.utmify[key];
}

function onyxAuthHeader() {
  if (!onyxPublicKey || !onyxPrivateKey) return "";
  return `Basic ${Buffer.from(`${onyxPublicKey}:${onyxPrivateKey}`).toString("base64")}`;
}

async function normalizeOnyxPix(data) {
  const tx = data && data.data ? data.data : data || {};
  const copyPaste = tx.pix_code || tx.pixCode || tx.copyPaste || tx.pix || tx.qrcode || tx.qr_code || "";
  let qr =
    tx.pix_qr_code ||
    tx.pixQrCode ||
    tx.qrCodeImage ||
    tx.qr_code_image ||
    tx.qr_code_base64 ||
    tx.qrcode_image ||
    "";
  if (!qr && copyPaste) {
    qr = await QRCode.toDataURL(copyPaste, { errorCorrectionLevel: "M", margin: 2, width: 260 });
  }
  return {
    transactionId: tx.id || tx.transaction_id || tx.transactionId,
    copyPaste,
    qrCodeImage: qr && !String(qr).startsWith("data:") ? `data:image/png;base64,${qr}` : qr,
    raw: tx,
  };
}

async function normalizeBlackcatPix(data) {
  const tx = data && data.data ? data.data : data || {};
  const payment = tx.paymentData || tx.pix || tx.payment || {};
  const copyPaste = payment.copyPaste || payment.qrCode || tx.copyPaste || tx.qrCode || "";
  let qr = payment.qrCodeBase64 || payment.qrCodeImage || tx.qrCodeBase64 || tx.qrCodeImage || "";
  if (!qr && copyPaste) {
    qr = await QRCode.toDataURL(copyPaste, { errorCorrectionLevel: "M", margin: 2, width: 260 });
  }
  return {
    transactionId: tx.transactionId || tx.id || tx.transaction_id,
    copyPaste,
    qrCodeImage: qr && !String(qr).startsWith("data:") ? `data:image/png;base64,${qr}` : qr,
    raw: tx,
  };
}

async function createOnyxPix(order, req) {
  const auth = onyxAuthHeader();
  if (!auth) throw new Error("Configure ONYXPAG_PUBLIC_KEY e ONYXPAG_PRIVATE_KEY no EasyPanel.");
  const customer = order.customer || {};
  const baseUrl = publicBaseUrl(req);
  const resolved = resolveProduct(order);
  const payload = {
    amount: resolved.total,
    payment_method: "pix",
    description: resolved.gatewayName,
    items: [
      {
        title: resolved.gatewayName,
        unitPrice: cents(resolved.total),
        quantity: 1,
        tangible: true,
      },
    ],
    customer: {
      name: customer.name,
      email: customer.email,
      document: customer.document,
      phone: customer.phone,
    },
    postbackUrl: `${baseUrl}/api/webhooks/onyxpag`,
    source_url: order.href || `${baseUrl}/checkout`,
  };
  const response = await fetch(onyxApiUrl, {
    method: "POST",
    headers: { "content-type": "application/json", Authorization: auth },
    body: JSON.stringify(payload),
  });
  const text = await response.text();
  let parsed = {};
  try {
    parsed = text ? JSON.parse(text) : {};
  } catch {
    parsed = { raw: text };
  }
  if (!response.ok || parsed.success === false) {
    throw new Error(`OnyxPag ${response.status}: ${text.slice(0, 500)}`);
  }
  return normalizeOnyxPix(parsed);
}

async function createBlackcatPix(order, req) {
  if (!blackcatApiKey) throw new Error("Configure BLACKCAT_API_KEY no EasyPanel para usar a Black Cat.");
  const customer = order.customer || {};
  const baseUrl = publicBaseUrl(req);
  const resolved = resolveProduct(order);
  const payload = {
    amount: cents(resolved.total),
    currency: "BRL",
    paymentMethod: "pix",
    items: [
      {
        title: resolved.gatewayName,
        unitPrice: cents(resolved.total),
        quantity: 1,
        tangible: true,
      },
    ],
    customer: {
      name: customer.name,
      email: customer.email,
      phone: customer.phone,
      document: {
        number: customer.document,
        type: String(customer.document || "").length > 11 ? "cnpj" : "cpf",
      },
    },
    shipping: {
      name: customer.name,
      street: customer.street,
      number: customer.number,
      complement: customer.complement || "",
      neighborhood: customer.neighborhood,
      city: customer.city,
      state: customer.state,
      zipCode: customer.zipcode,
    },
    pix: { expiresInDays: 1 },
    postbackUrl: `${baseUrl}/api/webhooks/blackcat`,
    externalRef: order.orderId,
  };
  const response = await fetch(`${blackcatApiUrl.replace(/\/+$/, "")}/sales/create-sale`, {
    method: "POST",
    headers: { "content-type": "application/json", "X-API-Key": blackcatApiKey },
    body: JSON.stringify(payload),
  });
  const text = await response.text();
  let parsed = {};
  try {
    parsed = text ? JSON.parse(text) : {};
  } catch {
    parsed = { raw: text };
  }
  if (!response.ok || parsed.success === false) {
    throw new Error(`Black Cat ${response.status}: ${text.slice(0, 500)}`);
  }
  return normalizeBlackcatPix(parsed);
}

async function createGatewayPix(order, req) {
  const gateway = getPaymentGateway();
  const pix = gateway === "blackcat" ? await createBlackcatPix(order, req) : await createOnyxPix(order, req);
  return { ...pix, gateway };
}

async function fetchGatewayPaymentStatus(order) {
  const gateway = order.paymentGateway || "onyxpag";
  const transactionId = order.gatewayTransactionId || order.orderId;
  if (!transactionId || !gatewayConfigured(gateway)) return null;
  let response;
  if (gateway === "blackcat") {
    response = await fetch(`${blackcatApiUrl.replace(/\/+$/, "")}/sales/${encodeURIComponent(transactionId)}/status`, {
      headers: { "X-API-Key": blackcatApiKey },
    });
  } else {
    response = await fetch(`${onyxApiUrl.replace(/\/+$/, "")}/transactions/${encodeURIComponent(transactionId)}`, {
      headers: { Authorization: onyxAuthHeader(), "content-type": "application/json" },
    });
  }
  const text = await response.text();
  let parsed = {};
  try {
    parsed = text ? JSON.parse(text) : {};
  } catch {
    parsed = {};
  }
  if (!response.ok || parsed.success === false) throw new Error(`${gatewayLabel(gateway)} status ${response.status}`);
  const data = parsed.data || parsed;
  return String(data.status || parsed.status || "").toLowerCase();
}

function authOk(req) {
  if (!adminPassword) return true;
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  const bearer = String(req.headers.authorization || "").replace(/^Bearer\s+/i, "");
  const cookies = parseCookies(req);
  const expectedSession = adminSessionValue();
  return (
    bearer === adminPassword ||
    (adminToken && bearer === adminToken) ||
    url.searchParams.get("token") === adminPassword ||
    (adminToken && url.searchParams.get("token") === adminToken) ||
    (expectedSession && cookies.admin_auth === expectedSession)
  );
}

function handleOptions(res) {
  res.writeHead(204, {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "content-type,authorization",
  });
  res.end();
}

async function handleTrack(req, res) {
  try {
    const body = await readBody(req);
    const { sessionId, event } = normalizePayload(body, req);
    const previous = store.sessions[sessionId] || {
      sessionId,
      firstSeenAt: event.createdAt,
      events: 0,
    };

    store.sessions[sessionId] = {
      ...previous,
      lastSeenAt: event.createdAt,
      lastStage: event.stage,
      orderId: event.orderId || previous.orderId,
      lastPage: event.page,
      product: event.product || previous.product,
      utmSource: event.utmSource || previous.utmSource,
      utmCampaign: event.utmCampaign || previous.utmCampaign,
      utmMedium: event.utmMedium || previous.utmMedium,
      referrer: event.referrer || previous.referrer,
      href: event.href || previous.href,
      userAgent: event.userAgent || previous.userAgent,
      screen: event.screen || previous.screen,
      ip: event.ip || previous.ip,
      customer: event.customer || previous.customer,
      shipping: event.shipping || previous.shipping,
      bump: event.bump || previous.bump,
      events: (previous.events || 0) + 1,
    };

    store.events.push(event);
    if (event.stage === "pix_code_copied" && event.orderId && store.orders && store.orders[event.orderId]) {
      const order = store.orders[event.orderId];
      order.copyClickedAt = order.copyClickedAt || event.createdAt;
      order.copyClicks = (order.copyClicks || 0) + 1;
      store.orders[event.orderId] = order;
      if (order.externalOrderId) store.orders[order.externalOrderId] = order;
    }
    if (store.events.length > MAX_EVENTS) store.events = store.events.slice(-MAX_EVENTS);

    scheduleSave();
    broadcast({ type: "update", event, summary: summarize().totals });
    json(res, 200, { ok: true, sessionId, eventId: event.id });
  } catch (error) {
    json(res, 400, { ok: false, error: error.message || "Invalid request" });
  }
}

async function handleCreatePix(req, res) {
  let lock;
  let orderId;
  let requestBody = {};
  try {
    const body = await readBody(req);
    requestBody = body;
    orderId = sanitizeText(body.orderId || body.id, 120) || crypto.randomUUID();
    const existingOrder = store.orders && store.orders[orderId];
    if (existingOrder && existingOrder.pix && existingOrder.pix.copyPaste) {
      return json(res, 200, { ok: true, orderId: existingOrder.orderId || existingOrder.id || orderId, order: existingOrder, pix: existingOrder.pix, reused: true });
    }

    const pendingCreation = pixCreationInFlight.get(orderId);
    if (pendingCreation) {
      await pendingCreation;
      const completedOrder = store.orders && store.orders[orderId];
      if (completedOrder && completedOrder.pix && completedOrder.pix.copyPaste) {
        return json(res, 200, {
          ok: true,
          orderId: completedOrder.orderId || completedOrder.id || orderId,
          order: completedOrder,
          pix: completedOrder.pix,
          reused: true,
        });
      }
      throw new Error("A geração anterior do Pix não foi concluída. Tente novamente.");
    }

    let releaseLock;
    const completion = new Promise((resolve) => {
      releaseLock = resolve;
    });
    lock = { completion, release: releaseLock };
    pixCreationInFlight.set(orderId, completion);

    const resolved = resolveProduct(body);
    const order = {
      ...body,
      orderId,
      id: orderId,
      product: resolved.name,
      productSlug: resolved.slug,
      selectedKit: resolved.variant,
      variant: resolved.variant,
      subtotal: resolved.subtotal,
      value: resolved.total,
      total: resolved.total,
      status: "pending_payment",
      paymentMethod: "pix",
      ip: getClientIp(req),
      createdAt: body.createdAt || new Date().toISOString(),
      tracking: cleanTracking(body.tracking || body),
    };
    const pix = await createGatewayPix(order, req);
    const finalOrderId = pix.transactionId || orderId;
    const savedOrder = {
      ...order,
      orderId: finalOrderId,
      id: finalOrderId,
      externalOrderId: orderId,
      paymentGateway: pix.gateway,
      gatewayTransactionId: pix.transactionId,
      onyxTransactionId: pix.gateway === "onyxpag" ? pix.transactionId : undefined,
      blackcatTransactionId: pix.gateway === "blackcat" ? pix.transactionId : undefined,
      pixGeneratedAt: new Date().toISOString(),
      pix: { copyPaste: pix.copyPaste, qrCodeImage: pix.qrCodeImage },
      gatewayRaw: pix.raw,
      onyx: pix.gateway === "onyxpag" ? pix.raw : undefined,
      blackcat: pix.gateway === "blackcat" ? pix.raw : undefined,
    };
    store.orders = store.orders || {};
    store.orders[finalOrderId] = savedOrder;
    store.orders[orderId] = savedOrder;
    if (savedOrder.sessionId) {
      const previousSession = store.sessions[savedOrder.sessionId] || { sessionId: savedOrder.sessionId, firstSeenAt: savedOrder.createdAt, events: 0 };
      store.sessions[savedOrder.sessionId] = {
        ...previousSession,
        lastSeenAt: savedOrder.pixGeneratedAt,
        lastStage: "pix_generated",
        orderId: finalOrderId,
        product: savedOrder.product || previousSession.product,
        paymentGateway: pix.gateway,
        customer: savedOrder.customer || previousSession.customer,
        shipping: savedOrder.shipping || previousSession.shipping,
        bump: savedOrder.bump || previousSession.bump,
        href: savedOrder.href || previousSession.href,
        events: previousSession.events || 0,
      };
    }
    const orderEvent = {
      id: crypto.randomUUID(),
      sessionId: savedOrder.sessionId,
      orderId: finalOrderId,
      type: "checkout",
      stage: "order_submitted",
      page: "checkout",
      product: resolved.name,
      productSlug: resolved.slug,
      selectedKit: resolved.variant,
      value: savedOrder.total,
      subtotal: savedOrder.subtotal,
      shipping: savedOrder.shipping,
      bump: savedOrder.bump,
      status: "pending_payment",
      paymentMethod: "pix",
      paymentGateway: pix.gateway,
      customer: savedOrder.customer,
      tracking: savedOrder.tracking,
      href: savedOrder.href,
      ip: savedOrder.ip,
      createdAt: savedOrder.createdAt,
    };
    const pixGeneratedEvent = {
      ...orderEvent,
      id: crypto.randomUUID(),
      stage: "pix_generated",
      page: "pedido",
      createdAt: savedOrder.pixGeneratedAt,
    };
    store.events.push(orderEvent, pixGeneratedEvent);
    if (store.events.length > MAX_EVENTS) store.events = store.events.slice(-MAX_EVENTS);
    await saveStoreNowAsync();
    syncUtmifyOrder(savedOrder, "waiting_payment").catch((error) => console.error("[utmify] waiting_payment:", error.message));
    broadcast({ type: "update", event: store.events[store.events.length - 1], summary: summarize().totals });
    json(res, 200, { ok: true, orderId: finalOrderId, order: savedOrder, pix: savedOrder.pix });
  } catch (error) {
    const safeError = sanitizeText(error.message || "Falha ao gerar Pix", 400);
    const resolved = resolveProduct(requestBody);
    const failedEvent = {
      id: crypto.randomUUID(),
      sessionId: sanitizeText(requestBody.sessionId, 120),
      orderId,
      type: "checkout",
      stage: "pix_generation_failed",
      page: "checkout",
      product: resolved.name,
      productSlug: resolved.slug,
      selectedKit: resolved.variant,
      value: resolved.total,
      paymentMethod: "pix",
      paymentGateway: getPaymentGateway(),
      customer: requestBody.customer,
      tracking: cleanTracking(requestBody.tracking || requestBody),
      href: sanitizeText(requestBody.href, 500),
      ip: getClientIp(req),
      error: safeError,
      createdAt: new Date().toISOString(),
    };
    store.events.push(failedEvent);
    if (store.events.length > MAX_EVENTS) store.events = store.events.slice(-MAX_EVENTS);
    if (failedEvent.sessionId && store.sessions[failedEvent.sessionId]) {
      store.sessions[failedEvent.sessionId].lastStage = failedEvent.stage;
      store.sessions[failedEvent.sessionId].lastError = safeError;
      store.sessions[failedEvent.sessionId].lastSeenAt = failedEvent.createdAt;
    }
    scheduleSave();
    console.error(`[checkout] ${failedEvent.paymentGateway} Pix failed:`, safeError);
    broadcast({ type: "update", event: failedEvent, summary: summarize().totals });
    json(res, 502, { ok: false, error: safeError });
  } finally {
    if (lock) {
      lock.release();
      if (pixCreationInFlight.get(orderId) === lock.completion) pixCreationInFlight.delete(orderId);
    }
  }
}

async function handleOrderState(req, res, url) {
  const orderId = decodeURIComponent(url.pathname.split("/").pop() || "");
  const order = store.orders && store.orders[orderId];
  if (!order) return json(res, 404, { ok: false, error: "Pedido não encontrado" });
  const now = Date.now();
  const lastCheck = Number(order.lastGatewayCheckAt || 0);
  if (order.status !== "paid" && now - lastCheck >= 10000) {
    order.lastGatewayCheckAt = now;
    try {
      const gatewayStatus = await fetchGatewayPaymentStatus(order);
      const paid = ["paid", "pago", "approved", "aprovado"].includes(gatewayStatus);
      if (gatewayStatus) order.paymentStatus = gatewayStatus;
      if (paid && order.status !== "paid") {
        order.status = "paid";
        order.paidAt = new Date().toISOString();
        const event = {
          id: crypto.randomUUID(),
          sessionId: order.sessionId,
          orderId: order.orderId,
          type: "checkout",
          stage: "payment_paid",
          page: "gateway_status",
          product: order.product || resolveProduct(order).name,
          productSlug: order.productSlug || resolveProduct(order).slug,
          value: order.total || order.value,
          status: "paid",
          paymentStatus: gatewayStatus,
          paymentMethod: "pix",
          paymentGateway: order.paymentGateway,
          customer: order.customer,
          createdAt: order.paidAt,
        };
        store.events.push(event);
        if (store.events.length > MAX_EVENTS) store.events = store.events.slice(-MAX_EVENTS);
        await saveStoreNowAsync();
        syncUtmifyOrder(order, "paid", order.paidAt).catch((error) => console.error("[utmify] paid:", error.message));
        broadcast({ type: "update", event, summary: summarize().totals });
      } else {
        scheduleSave();
      }
    } catch (error) {
      order.lastGatewayStatusError = error.message;
      scheduleSave();
    }
  }
  json(res, 200, { ok: true, order });
}

async function handleGatewayWebhook(req, res, gateway = "onyxpag") {
  try {
    const body = await readBody(req);
    const data = body.data || body;
    let metadata = data.metadata || body.metadata || {};
    if (typeof metadata === "string") {
      try {
        metadata = JSON.parse(metadata);
      } catch {
        metadata = {};
      }
    }
    const transactionId = sanitizeText(data.transaction_id || data.transactionId || data.id || body.transaction_id || body.transactionId, 120);
    const externalId = sanitizeText(
      data.external_id ||
        data.external_ref ||
        data.externalReference ||
        data.externalRef ||
        body.externalReference ||
        body.externalRef ||
        (metadata && metadata.order_id),
      120
    );
    const status = String(data.status || body.status || "").toLowerCase();
    const paid =
      status === "paid" ||
      status === "pago" ||
      status === "approved" ||
      status === "aprovado" ||
      body.event === "transaction.paid" ||
      body.event === "payment.paid";
    const orderKey = transactionId || externalId;
    const amount = gateway === "blackcat" ? Number(data.amount || body.amount || 0) / 100 : Number(data.amount || 0);
    const order = (store.orders && (store.orders[transactionId] || store.orders[externalId])) || {
      orderId: orderKey,
      id: orderKey,
      total: amount,
      value: amount,
      paymentMethod: "pix",
      paymentGateway: gateway,
      customer: data.customer || {},
      createdAt: new Date().toISOString(),
    };
    order.paymentGateway = order.paymentGateway || gateway;
    order.status = paid ? "paid" : status || order.status || "pending_payment";
    order.paymentStatus = order.status;
    order.paidAt = paid ? new Date().toISOString() : order.paidAt;
    store.orders = store.orders || {};
    if (transactionId) store.orders[transactionId] = order;
    if (externalId) store.orders[externalId] = order;
    const event = {
      id: crypto.randomUUID(),
      sessionId: order.sessionId,
      orderId: order.orderId || orderKey,
      type: "checkout",
      stage: paid ? "payment_paid" : "payment_update",
      page: "webhook",
      product: order.product || resolveProduct(order).name,
      productSlug: order.productSlug || resolveProduct(order).slug,
      value: order.total || order.value,
      status: order.status,
      paymentStatus: order.paymentStatus,
      paymentMethod: "pix",
      paymentGateway: order.paymentGateway,
      customer: order.customer,
      createdAt: new Date().toISOString(),
    };
    store.events.push(event);
    if (store.events.length > MAX_EVENTS) store.events = store.events.slice(-MAX_EVENTS);
    scheduleSave();
    if (paid) syncUtmifyOrder(order, "paid", order.paidAt).catch((error) => console.error("[utmify] paid:", error.message));
    broadcast({ type: "update", event, summary: summarize().totals });
    json(res, 200, { ok: true, status: "received" });
  } catch (error) {
    json(res, 400, { ok: false, error: error.message || "Webhook inválido" });
  }
}

async function handleOnyxWebhook(req, res) {
  return handleGatewayWebhook(req, res, "onyxpag");
}

async function handleBlackcatWebhook(req, res) {
  return handleGatewayWebhook(req, res, "blackcat");
}

function handleAdminState(req, res) {
  if (!authOk(req)) return json(res, 401, { ok: false, error: "Unauthorized" });
  json(res, 200, { ok: true, ...summarize(), protected: Boolean(adminPassword), settings: getSettings(), gateways: gatewaySummary() });
}

function gatewaySummary() {
  const selected = getPaymentGateway();
  return {
    selected,
    options: [
      { id: "onyxpag", name: "OnyxPag", configured: gatewayConfigured("onyxpag"), endpoint: onyxApiUrl },
      {
        id: "blackcat",
        name: "Black Cat",
        configured: gatewayConfigured("blackcat"),
        endpoint: blackcatApiUrl,
        publicKeyConfigured: Boolean(blackcatPublicKey),
        splitConfigured: Boolean(blackcatSplitCode),
      },
    ],
  };
}

async function handleAdminGateway(req, res) {
  if (!authOk(req)) return json(res, 401, { ok: false, error: "Unauthorized" });
  if (req.method === "GET") return json(res, 200, { ok: true, gateways: gatewaySummary() });
  if (req.method !== "POST") return json(res, 405, { ok: false, error: "Method not allowed" });
  const body = await readBody(req);
  const gateway = sanitizeText(body.gateway, 40);
  if (!["onyxpag", "blackcat"].includes(gateway)) return json(res, 400, { ok: false, error: "Gateway inválido" });
  if (!gatewayConfigured(gateway)) return json(res, 400, { ok: false, error: `${gatewayLabel(gateway)} ainda não está configurado no EasyPanel` });
  getSettings().paymentGateway = gateway;
  scheduleSave();
  const event = {
    id: crypto.randomUUID(),
    type: "admin",
    stage: "payment_gateway_changed",
    page: "admin",
    paymentGateway: gateway,
    createdAt: new Date().toISOString(),
  };
  store.events.push(event);
  if (store.events.length > MAX_EVENTS) store.events = store.events.slice(-MAX_EVENTS);
  broadcast({ type: "update", event, summary: summarize().totals });
  json(res, 200, { ok: true, gateways: gatewaySummary(), message: `Gateway alterado para ${gatewayLabel(gateway)}` });
}

async function handleAdminLogin(req, res) {
  if (req.method !== "POST") return json(res, 405, { ok: false, error: "Method not allowed" });
  if (!adminPassword) return json(res, 200, { ok: true, protected: false, message: "Painel sem senha configurada." });
  const body = await readBody(req);
  const username = sanitizeText(body.username, 120) || "";
  const password = String(body.password || "");
  if (username !== adminUser || password !== adminPassword) {
    clearAdminCookie(res);
    return json(res, 401, { ok: false, error: "Usuário ou senha inválidos." });
  }
  setAdminCookie(req, res);
  json(res, 200, { ok: true, protected: true, message: "Login realizado." });
}

function handleAdminLogout(req, res) {
  clearAdminCookie(res);
  json(res, 200, { ok: true });
}

function handleIntegrationStatus(req, res) {
  json(res, 200, {
    ok: true,
    server: "CamVision Node",
    time: new Date().toISOString(),
    data: {
      dir: dataDir,
      file: dataFile,
      maxEvents: MAX_EVENTS,
      orders: Object.keys(store.orders || {}).length,
      events: (store.events || []).length,
      sessions: Object.keys(store.sessions || {}).length,
    },
    database: {
      configured: Boolean(databaseUrl),
      connected: databaseReady,
      table: databaseUrl ? "app_store" : null,
    },
    paymentGateway: getPaymentGateway(),
    onyxpag: {
      configured: Boolean(onyxPublicKey && onyxPrivateKey),
      endpoint: onyxApiUrl,
    },
    blackcat: {
      configured: Boolean(blackcatApiKey),
      publicKeyConfigured: Boolean(blackcatPublicKey),
      splitConfigured: Boolean(blackcatSplitCode),
      endpoint: blackcatApiUrl,
    },
    utmify: {
      configured: Boolean(utmifyApiToken),
      endpoint: utmifyApiUrl,
    },
    timezone: appTimezone,
  });
}

function csvCell(value) {
  const text = value == null ? "" : String(value);
  return `"${text.replace(/"/g, '""')}"`;
}

function handleAdminExport(req, res, url) {
  if (!authOk(req)) return json(res, 401, { ok: false, error: "Unauthorized" });
  const format = url.searchParams.get("format") || "json";
  const snapshot = summarize();
  const stamp = new Date().toISOString().slice(0, 10);
  if (format === "csv") {
    const rows = [
      ["data", "pedido", "gateway", "cliente", "email", "telefone", "produto", "valor", "status", "pix_gerado", "copiou_pix", "pagou"].map(csvCell).join(","),
      ...(snapshot.orders || []).map((order) =>
        [
          order.createdAt,
          order.orderId || order.id,
          order.paymentGateway,
          order.customer && order.customer.name,
          order.customer && order.customer.email,
          order.customer && order.customer.phone,
          order.product,
          order.value,
          order.status,
          order.pixGeneratedAt,
          order.copyClickedAt,
          order.paidAt,
        ]
          .map(csvCell)
          .join(",")
      ),
    ];
    return sendDownload(res, `casaorganizy-pedidos-${stamp}.csv`, "text/csv; charset=utf-8", rows.join("\n"));
  }
  sendDownload(
    res,
    `casaorganizy-backup-${stamp}.json`,
    "application/json; charset=utf-8",
    JSON.stringify({ exportedAt: new Date().toISOString(), dataFile, ...snapshot }, null, 2)
  );
}

function handleStream(req, res) {
  if (!authOk(req)) return json(res, 401, { ok: false, error: "Unauthorized" });
  res.writeHead(200, {
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-store",
    connection: "keep-alive",
    "x-accel-buffering": "no",
  });
  res.write(`data: ${JSON.stringify({ type: "snapshot", ...summarize() })}\n\n`);
  clients.add(res);
  req.on("close", () => clients.delete(res));
}

function safeStaticPath(urlPath) {
  const decoded = decodeURIComponent(urlPath.split("?")[0]);
  const clean = path.normalize(decoded).replace(/^(\.\.[/\\])+/, "");
  return path.join(root, clean === "/" ? "index.html" : clean);
}

function serveFile(req, res, filePath) {
  const resolved = path.resolve(filePath);
  if (!resolved.startsWith(root)) {
    res.writeHead(403);
    return res.end("Forbidden");
  }
  fs.stat(resolved, (err, stat) => {
    if (err || !stat.isFile()) {
      const fallback = path.join(root, "index.html");
      return serveFile(req, res, fallback);
    }
    const ext = path.extname(resolved).toLowerCase();
    const type = mime[ext] || "application/octet-stream";
    const compressible = [".html", ".css", ".js", ".json", ".svg", ".txt"].includes(ext);
    const acceptEncoding = String(req.headers["accept-encoding"] || "");
    const etag = `W/"${stat.size.toString(16)}-${Math.trunc(stat.mtimeMs).toString(16)}"`;
    const headers = {
      "content-type": type,
      "cache-control": ext === ".html" ? "no-cache" : "public, max-age=31536000, immutable",
      etag,
      "last-modified": stat.mtime.toUTCString(),
      vary: compressible ? "Accept-Encoding" : undefined,
    };
    Object.keys(headers).forEach((key) => headers[key] === undefined && delete headers[key]);
    if (req.headers["if-none-match"] === etag) {
      res.writeHead(304, headers);
      return res.end();
    }
    if (req.method === "HEAD") {
      res.writeHead(200, { ...headers, "content-length": stat.size });
      return res.end();
    }
    if (compressible && stat.size > 1024 && /\bbr\b/.test(acceptEncoding)) {
      res.writeHead(200, { ...headers, "content-encoding": "br" });
      const brotli = zlib.createBrotliCompress({
        params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 4 },
      });
      return fs.createReadStream(resolved).pipe(brotli).pipe(res);
    }
    if (compressible && stat.size > 1024 && /\bgzip\b/.test(acceptEncoding)) {
      res.writeHead(200, { ...headers, "content-encoding": "gzip" });
      return fs.createReadStream(resolved).pipe(zlib.createGzip({ level: 6 })).pipe(res);
    }
    res.writeHead(200, { ...headers, "content-length": stat.size });
    fs.createReadStream(resolved).pipe(res);
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

  if (req.method === "OPTIONS") return handleOptions(res);

  if (url.pathname === "/health") return json(res, 200, { ok: true });
  if (url.pathname === "/api/public/track" && req.method === "POST") return handleTrack(req, res);
  if (url.pathname === "/api/integrations/status" && req.method === "GET") return handleIntegrationStatus(req, res);
  if (url.pathname === "/integrations/status" && req.method === "GET") return handleIntegrationStatus(req, res);
  if (url.pathname === "/api/checkout/create-pix" && req.method === "POST") return handleCreatePix(req, res);
  if (url.pathname === "/checkout/create-pix" && req.method === "POST") return handleCreatePix(req, res);
  if (url.pathname === "/create-pix-order" && req.method === "POST") return handleCreatePix(req, res);
  if (url.pathname.startsWith("/api/orders/") && req.method === "GET") return handleOrderState(req, res, url);
  if (url.pathname === "/api/webhooks/onyxpag" && req.method === "POST") return handleOnyxWebhook(req, res);
  if (url.pathname === "/webhooks/onyxpag" && req.method === "POST") return handleOnyxWebhook(req, res);
  if (url.pathname === "/api/webhooks/blackcat" && req.method === "POST") return handleBlackcatWebhook(req, res);
  if (url.pathname === "/webhooks/blackcat" && req.method === "POST") return handleBlackcatWebhook(req, res);
  if (url.pathname === "/api/admin/state" && req.method === "GET") return handleAdminState(req, res);
  if (url.pathname === "/api/admin/export" && req.method === "GET") return handleAdminExport(req, res, url);
  if (url.pathname === "/api/admin/login") return handleAdminLogin(req, res);
  if (url.pathname === "/api/admin/logout" && req.method === "POST") return handleAdminLogout(req, res);
  if (url.pathname === "/api/admin/gateway") return handleAdminGateway(req, res);
  if (url.pathname === "/api/admin/stream" && req.method === "GET") return handleStream(req, res);
  if (url.pathname === "/admin") return serveFile(req, res, path.join(root, "admin.html"));
  if (url.pathname === "/checkout") return serveFile(req, res, path.join(root, "checkout.html"));
  if (["/produto", "/produto/", "/kit-pote", "/kit-pote/", "/kit-jogo-de-cama", "/kit-jogo-de-cama/", "/escova-limpeza", "/escova-limpeza/", "/colchao-inflavel", "/colchao-inflavel/"].includes(url.pathname)) {
    res.writeHead(302, { location: "/" });
    return res.end();
  }
  if (url.pathname === "/pedido" || url.pathname.startsWith("/pedido/")) return serveFile(req, res, path.join(root, "pedido.html"));

  return serveFile(req, res, safeStaticPath(url.pathname));
});

async function shutdown() {
  try {
    await saveStoreNowAsync();
    if (pgPool) await pgPool.end();
  } catch (error) {
    console.error("[shutdown] failed:", error.message);
  } finally {
    process.exit(0);
  }
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

async function start() {
  loadStore();
  await initDatabase();
  server.listen(port, "0.0.0.0", () => {
    console.log(`CamVision server listening on ${port}`);
    console.log(`[store] file=${dataFile}`);
    console.log(`[postgres] ${databaseUrl ? (databaseReady ? "connected" : "configured but not connected") : "not configured"}`);
  });
}

start().catch((error) => {
  console.error("[startup] failed:", error.message);
  process.exit(1);
});
