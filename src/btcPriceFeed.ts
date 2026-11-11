import WebSocket from "ws";

const WS_URL = "wss://ws-live-v2.polymarket.com/ws";
const SYMBOL = "btcusd";
const NAME = "btcPriceFeed";
const CHANNEL = "price.crypto.twap";
const PING_INTERVAL_MS = 20 * 1000;
const BUFFER_MS = 10 * 60 * 1000;
const RECONNECT_MS = 2000;
const STALE_TIMEOUT_MS = 60 * 1000;
const WATCHDOG_CHECK_MS = 10 * 1000;

interface Sample {
  ts: number;
  price: number;
}

class BtcPriceFeed {
  private ws: WebSocket | null = null;
  private buffer: Sample[] = [];
  private stopped = false;
  private pingTimer: NodeJS.Timeout | null = null;
  private watchdogTimer: NodeJS.Timeout | null = null;
  private lastDataMessageAt = Date.now();

  start(): void {
    this.connect();
    this.startWatchdog();
  }

  private addSample(ts: number, price: number): void {
    if (!Number.isFinite(ts) || !Number.isFinite(price)) return;
    this.buffer.push({ ts, price });
    const cutoff = Date.now() - BUFFER_MS;
    while (this.buffer.length && this.buffer[0].ts < cutoff) this.buffer.shift();
  }

  private connect(): void {
    if (this.stopped) return;
    this.lastDataMessageAt = Date.now();
    this.ws = new WebSocket(WS_URL);

    this.ws.on("open", () => {
      console.log(`[${NAME}] подключено к Polymarket PolyBolt (Chainlink TWAP 60s)`);
      this.lastDataMessageAt = Date.now();
      const key = process.env.CLOB_API_KEY;
      const secret = process.env.CLOB_API_SECRET;
      const passphrase = process.env.CLOB_API_PASSPHRASE;
      if (!key || !secret || !passphrase) {
        console.error(`[${NAME}] ❌ нет CLOB_API_KEY / CLOB_API_SECRET / CLOB_API_PASSPHRASE в переменных`);
        return;
      }
      this.ws!.send(JSON.stringify({ op: "auth", rid: "a1", auth: { apiKey: key, secret, passphrase } }));
      this.startPing();
    });

    this.ws.on("message", (raw: Buffer) => {
      try {
        const msg = JSON.parse(raw.toString());
        if (msg.op === "authed") {
          this.ws!.send(
            JSON.stringify({
              op: "subscribe",
              rid: "s1",
              subscriptions: [{ channel: CHANNEL, filter: { symbol: SYMBOL, window_seconds: 60 } }],
            }),
          );
          return;
        }
        if (msg.op === "error") {
          console.error(`[${NAME}] ошибка сервера: ${msg.code}`);
          return;
        }
        if (msg.channel !== CHANNEL || !msg.payload) return;
        if (msg.payload.symbol !== SYMBOL) return;

        if (msg.snapshot && Array.isArray(msg.payload.data)) {
          for (const d of msg.payload.data) this.addSample(Number(d.timestamp), Number(d.value));
        } else {
          this.addSample(Number(msg.payload.timestamp), Number(msg.payload.value));
        }
        this.lastDataMessageAt = Date.now();
      } catch {
        // игнорируем неожиданные сообщения
      }
    });

    this.ws.on("close", (code) => {
      if (this.stopped) return;
      console.log(`[${NAME}] соединение закрыто (код ${code}), переподключение через ${RECONNECT_MS}мс`);
      this.stopPing();
      setTimeout(() => this.connect(), RECONNECT_MS);
    });

    this.ws.on("error", (err) => {
      console.error(`[${NAME}] ошибка:`, err.message);
    });
  }

  private startPing(): void {
    this.stopPing();
    this.pingTimer = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        try {
          this.ws.send(JSON.stringify({ op: "ping" }));
        } catch {
          // watchdog поймает
        }
      }
    }, PING_INTERVAL_MS);
  }

  private stopPing(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  private startWatchdog(): void {
    this.watchdogTimer = setInterval(() => {
      if (this.stopped) return;
      const idleMs = Date.now() - this.lastDataMessageAt;
      if (idleMs > STALE_TIMEOUT_MS) {
        console.log(`[${NAME}] ⚠️ Нет TWAP-апдейтов ${Math.round(idleMs / 1000)}с — форсируем переподключение.`);
        this.forceReconnect();
      }
    }, WATCHDOG_CHECK_MS);
  }

  private forceReconnect(): void {
    this.lastDataMessageAt = Date.now();
    this.stopPing();
    try {
      this.ws?.removeAllListeners();
      this.ws?.terminate();
    } catch (err) {
      console.error(`[${NAME}] ошибка при terminate:`, (err as Error).message);
    }
    this.connect();
  }

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

  stop(): void {
    this.stopped = true;
    this.stopPing();
    if (this.watchdogTimer) {
      clearInterval(this.watchdogTimer);
      this.watchdogTimer = null;
    }
    this.ws?.close();
  }
}

export const btcPriceFeed = new BtcPriceFeed();
