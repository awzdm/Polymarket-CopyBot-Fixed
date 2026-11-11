/**
 * Фид цен монет через Polymarket PolyBolt (замена RTDS).
 *
 * Зачем: RTDS-топик crypto_prices_twap_thirty (Chainlink TWAP 30с) у Polymarket
 * больше не отдаёт данные, замены у него нет. Новый поток — PolyBolt:
 *   wss://ws-live-v2.polymarket.com/ws , канал price.crypto (сырой Chainlink-тик,
 *   ~раз в секунду), символы вида btcusd. Канал закрытый: нужна авторизация
 *   ключами CLOB (те же CLOB_API_KEY / CLOB_API_SECRET / CLOB_API_PASSPHRASE).
 *
 * Чтобы стратегии продолжали видеть ту же цену, что раньше, TWAP-30с считаем
 * сами: время-взвешенное среднее тиков за последние 30 секунд. Интерфейс тот же
 * (start/stop/getPriceAt/getLatestPrice), остальной код бота менять не нужно.
 *
 * Одно соединение на все 5 монет. Watchdog: нет данных 60с — переподключение.
 */
import WebSocket from "ws";

const WS_URL = process.env.POLYBOLT_WS_URL ?? "wss://ws-live-v2.polymarket.com/ws";
const TWAP_MS = 30 * 1000;
const BUFFER_MS = 10 * 60 * 1000;
const RECONNECT_MS = 2000;
const STALE_TIMEOUT_MS = 60 * 1000;
const WATCHDOG_CHECK_MS = 10 * 1000;
const SUB_FALLBACK_MS = 1500; // если ack авторизации не распознали — подписываемся сами через это время

interface Sample {
  ts: number;
  price: number;
}

export class CoinFeed {
  private raw: Sample[] = [];
  private buffer: Sample[] = [];
  lastDataAt = Date.now();
  private firstLogged = false;

  constructor(
    readonly symbol: string, // "btcusd"
    readonly tag: string, // "btcPriceFeed" — для логов
  ) {}

  start(): void {
    hub.register(this);
  }

  stop(): void {
    hub.unregister(this);
  }

  /** Принять сырой тик (ts в мс). */
  ingest(tsIn: number, price: number, source?: string): void {
    if (!Number.isFinite(tsIn) || !Number.isFinite(price) || price <= 0) return;
    const ts = tsIn < 1e12 ? tsIn * 1000 : tsIn;
    const last = this.raw.length ? this.raw[this.raw.length - 1] : null;
    if (last && ts <= last.ts) return; // дубль или старый тик (например, из snapshot после реконнекта)
    this.raw.push({ ts, price });
    const cutoff = ts - BUFFER_MS - TWAP_MS;
    while (this.raw.length > 1 && this.raw[0].ts < cutoff) this.raw.shift();
    const tw = this.twapAt(ts);
    this.buffer.push({ ts, price: tw });
    const c2 = Date.now() - BUFFER_MS;
    while (this.buffer.length && this.buffer[0].ts < c2) this.buffer.shift();
    this.lastDataAt = Date.now();
    if (!this.firstLogged) {
      this.firstLogged = true;
      console.log(`[${this.tag}] первый тик PolyBolt: ${price} (источник: ${source ?? "?"}), TWAP30=${tw.toFixed(6)}`);
      if (source && source.toLowerCase() !== "chainlink") {
        console.log(`[${this.tag}] ⚠️ источник цены не chainlink (${source}) — рынки резолвятся по Chainlink, проверь.`);
      }
    }
  }

  /** Время-взвешенное среднее тиков в окне (t-30с, t]. */
  private twapAt(t: number): number {
    const w0 = t - TWAP_MS;
    let sum = 0;
    let covered = 0;
    let end = t;
    for (let i = this.raw.length - 1; i >= 0; i--) {
      const s = this.raw[i];
      const start = Math.max(s.ts, w0);
      if (end > start) {
        sum += s.price * (end - start);
        covered += end - start;
      }
      end = start;
      if (s.ts <= w0) break;
    }
    return covered > 0 ? sum / covered : this.raw[this.raw.length - 1].price;
  }

  /** Цена (TWAP-30с), ближайшая по времени к ts. null — данных ещё нет. */
  getPriceAt(ts: number): number | null {
    if (this.buffer.length === 0) return null;
    let closest = this.buffer[0];
    for (const s of this.buffer) {
      if (Math.abs(s.ts - ts) < Math.abs(closest.ts - ts)) closest = s;
      if (s.ts > ts) break;
    }
    return closest.price;
  }

  getLatestPrice(): number | null {
    return this.buffer.length ? this.buffer[this.buffer.length - 1].price : null;
  }
}

class PolyboltHub {
  private feeds = new Map<string, CoinFeed>();
  private ws: WebSocket | null = null;
  private started = false;
  private stopped = false;
  private subscribed = false;
  private useProvider = true;
  private subTimer: NodeJS.Timeout | null = null;
  private watchdog: NodeJS.Timeout | null = null;
  private lastAnyAt = Date.now();
  private ctrlLogged = 0;

  register(f: CoinFeed): void {
    this.feeds.set(f.symbol, f);
    this.stopped = false;
    if (!this.started) {
      this.started = true;
      this.connect();
      this.watchdog = setInterval(() => this.checkStale(), WATCHDOG_CHECK_MS);
    } else if (this.subscribed) {
      this.sendSubscribe([f]);
    }
  }

  unregister(f: CoinFeed): void {
    this.feeds.delete(f.symbol);
    if (this.feeds.size === 0) {
      this.stopped = true;
      this.started = false;
      if (this.watchdog) clearInterval(this.watchdog);
      this.watchdog = null;
      this.ws?.close();
    }
  }

  private storedCreds: { apiKey: string; secret: string; passphrase: string } | null = null;
  private authSent = false;

  /** Ключи CLOB, которые бот получил при запуске (createOrDeriveApiKey). Можно вызвать уже после подключения. */
  setCreds(c: { key: string; secret: string; passphrase: string }): void {
    this.storedCreds = { apiKey: c.key, secret: c.secret, passphrase: c.passphrase };
    if (this.ws && this.ws.readyState === WebSocket.OPEN && !this.authSent) this.sendAuth();
  }

  private sendAuth(): void {
    const c = this.creds();
    if (!c || !this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    this.authSent = true;
    this.ws.send(JSON.stringify({ op: "auth", rid: "a1", auth: c }));
    this.clearSubTimer();
    this.subTimer = setTimeout(() => this.sendSubscribe([...this.feeds.values()], true), SUB_FALLBACK_MS);
  }

  private creds(): { apiKey: string; secret: string; passphrase: string } | null {
    if (this.storedCreds) return this.storedCreds;
    const apiKey = process.env.CLOB_API_KEY;
    const secret = process.env.CLOB_API_SECRET;
    const passphrase = process.env.CLOB_API_PASSPHRASE;
    return apiKey && secret && passphrase ? { apiKey, secret, passphrase } : null;
  }

  private connect(): void {
    if (this.stopped) return;
    this.subscribed = false;
    this.lastAnyAt = Date.now();
    const ws = new WebSocket(WS_URL);
    this.ws = ws;

    ws.on("open", () => {
      console.log("[polybolt] подключено к Polymarket PolyBolt (Chainlink, TWAP-30с считаем сами)");
      this.authSent = false;
      if (this.creds()) this.sendAuth();
      else console.log("[polybolt] жду ключи CLOB (их передаёт бот после запуска или CLOB_API_KEY/SECRET/PASSPHRASE) — каналы цен закрытые.");
    });

    ws.on("message", (raw: Buffer) => this.onMessage(raw));

    ws.on("close", (code: number) => {
      if (this.stopped) return;
      console.log(`[polybolt] соединение закрыто (код ${code}), переподключение через ${RECONNECT_MS}мс`);
      this.clearSubTimer();
      this.subscribed = false;
      this.authSent = false;
      setTimeout(() => this.connect(), RECONNECT_MS + (code === 4003 ? Math.random() * 10000 : 0));
    });

    ws.on("error", (err: Error) => {
      console.error("[polybolt] ошибка:", err.message);
    });
  }

  private clearSubTimer(): void {
    if (this.subTimer) clearTimeout(this.subTimer);
    this.subTimer = null;
  }

  private sendSubscribe(list: CoinFeed[], first = false): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN || list.length === 0) return;
    if (first) {
      if (this.subscribed) return;
      this.subscribed = true;
    }
    const subscriptions = list.map((f) => ({
      channel: "price.crypto",
      filter: this.useProvider ? { symbol: f.symbol, provider: "chainlink" } : { symbol: f.symbol },
    }));
    try {
      this.ws.send(JSON.stringify({ op: "subscribe", rid: "s1", subscriptions }));
    } catch (e) {
      console.error("[polybolt] не удалось отправить подписку:", (e as Error).message);
    }
  }

  private onMessage(raw: Buffer): void {
    let msg: any;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (msg && msg.channel === "price.crypto" && msg.payload) {
      const p = msg.payload;
      const f = this.feeds.get(String(p.symbol ?? "").toLowerCase());
      if (!f) return;
      this.lastAnyAt = Date.now();
      if (Array.isArray(p.data)) {
        for (const d of p.data) f.ingest(Number(d.timestamp), Number(d.full_accuracy_value ?? d.value), p.source);
      } else {
        f.ingest(Number(p.timestamp ?? msg.ts), Number(p.full_accuracy_value ?? p.value), p.source);
      }
      return;
    }
    // служебные кадры: ack/ошибки — логируем (первые 25 + все ошибки), чтобы было видно причину
    const s = JSON.stringify(msg);
    const isErr = /error|auth_|bad_|rate_limited|sub_limit/i.test(s);
    if (isErr || this.ctrlLogged < 25) {
      this.ctrlLogged++;
      console.log(`[polybolt] служебное: ${s.slice(0, 300)}`);
    }
    if (/bad_filter/i.test(s) && this.useProvider) {
      this.useProvider = false;
      console.log("[polybolt] фильтр provider не принят — подписываюсь без него (источник по умолчанию).");
      this.subscribed = false;
      this.sendSubscribe([...this.feeds.values()], true);
      return;
    }
    // ack авторизации (rid a1) без ошибки → подписываемся сразу
    if (!isErr && msg && msg.rid === "a1" && !this.subscribed) {
      this.clearSubTimer();
      this.sendSubscribe([...this.feeds.values()], true);
    }
  }

  private checkStale(): void {
    if (this.stopped) return;
    const idle = Date.now() - this.lastAnyAt;
    if (idle > STALE_TIMEOUT_MS) {
      console.log(`[polybolt] ⚠️ Нет цен ${Math.round(idle / 1000)}с — форсируем переподключение.`);
      this.lastAnyAt = Date.now();
      this.clearSubTimer();
      try {
        this.ws?.removeAllListeners();
        this.ws?.terminate();
      } catch (err) {
        console.error("[polybolt] ошибка при terminate:", (err as Error).message);
      }
      this.connect();
    }
  }
}

const hub = new PolyboltHub();

/** Передать фиду ключи CLOB от бота (ClobService.getApiCreds()). */
export function setPolyboltCreds(c: { key: string; secret: string; passphrase: string }): void {
  hub.setCreds(c);
}
