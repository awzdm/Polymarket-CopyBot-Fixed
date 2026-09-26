/**
 * Фид цены BTC через Polymarket RTDS (Realtime Data Stream) — это
 * официальный бесплатный WebSocket Polymarket, транслирующий
 * Chainlink-вычисленный TWAP (time-weighted average price). Доступ
 * БЕЗ API-ключей и регистрации.
 *
 * Используем 30-секундное TWAP-окно (topic: crypto_prices_twap_thirty) —
 * это ближе всего к "реальному времени" из того, что публично отдаёт
 * RTDS, и именно Chainlink-данные, а не биржевой спот (Binance).
 *
 * Документация: https://docs.polymarket.com/market-data/chainlink-twap
 *
 * ВАЖНО: TWAP — это среднее за последние 30 секунд, а не мгновенный тик.
 * Он немного "смазан" по времени относительно чистого спота, но зато
 * гораздо ближе к тому, что реально видит Polymarket при резолве, и
 * дополнительно сам по себе сглаживает шум/дребезг.
 *
 * v2: ДОБАВЛЕН WATCHDOG (та же идея, что в priceWatcher.ts). Сокет может
 * формально остаться "открытым" (close/error не срабатывают), но реальные
 * TWAP-апдейты перестанут приходить — например из-за NAT/прокси-таймаута
 * на простое. Без watchdog'а бот молча продолжал бы торговать на
 * устаревшей цене монеты, думая что всё в порядке. Watchdog следит ТОЛЬКО
 * за реальными сообщениями с обновлением цены (topic/type/symbol совпали
 * и цена успешно распарсилась) — не за любым входящим сырым сообщением и
 * не за фактом, что мы сами шлём "PING".
 */

import WebSocket from "ws";

const WS_URL = "wss://ws-live-data.polymarket.com";
const SYMBOL = "btc/usd";
const TWAP_TOPIC = "crypto_prices_twap_thirty"; // 30-секундное окно
const PING_INTERVAL_MS = 5 * 1000; // RTDS требует текстовый пинг "PING" каждые 5с
const BUFFER_MS = 10 * 60 * 1000;
const RECONNECT_MS = 2000;

// Watchdog: если так долго нет РЕАЛЬНЫХ TWAP-апдейтов — считаем фид
// зависшим и форсируем переподключение. TWAP-окно 30с, апдейты обычно
// идут заметно чаще — 60с тишины это уже явная аномалия, а не штатная пауза.
const STALE_TIMEOUT_MS = 60 * 1000;
const WATCHDOG_CHECK_MS = 10 * 1000;

interface Sample {
  ts: number; // время наблюдения Chainlink (payload.timestamp)
  price: number;
}

interface TwapUpdateMessage {
  topic: string;
  type: string;
  timestamp: number;
  payload: {
    symbol: string;
    value: number;
    full_accuracy_value?: string;
    timestamp: number;
    window_s: number;
  };
}

class BtcPriceFeed {
  private ws: WebSocket | null = null;
  private buffer: Sample[] = [];
  private stopped = false;
  private pingTimer: NodeJS.Timeout | null = null;
  private watchdogTimer: NodeJS.Timeout | null = null;
  // Обновляется ТОЛЬКО когда реально пришёл и распарсился TWAP-апдейт по
  // нашему символу — не на любое сообщение и не на факт отправки PING.
  private lastDataMessageAt = Date.now();

  start(): void {
    this.connect();
    this.startWatchdog();
  }

  private connect(): void {
    if (this.stopped) return;

    this.lastDataMessageAt = Date.now(); // не считаем зависшим сразу после (пере)подключения
    this.ws = new WebSocket(WS_URL);

    this.ws.on("open", () => {
      console.log("[btcPriceFeed] подключено к Polymarket RTDS (Chainlink TWAP 30s)");
      this.lastDataMessageAt = Date.now();
      const subscribeMsg = {
        action: "subscribe",
        subscriptions: [
          {
            topic: TWAP_TOPIC,
            type: "update",
            filters: JSON.stringify({ symbol: SYMBOL }),
          },
        ],
      };
      this.ws!.send(JSON.stringify(subscribeMsg));
      this.startPing();
    });

    this.ws.on("message", (raw: Buffer) => {
      try {
        const msg = JSON.parse(raw.toString()) as Partial<TwapUpdateMessage>;
        if (msg.topic !== TWAP_TOPIC || msg.type !== "update" || !msg.payload) return;
        if (msg.payload.symbol !== SYMBOL) return;

        const price = Number(msg.payload.value);
        const ts = Number(msg.payload.timestamp);
        if (!Number.isFinite(price) || !Number.isFinite(ts)) return;

        // Реальный, валидный TWAP-апдейт по нашему символу — вот это и
        // есть признак того, что фид живой, а не просто "сокет отвечает".
        this.lastDataMessageAt = Date.now();

        this.buffer.push({ ts, price });
        const cutoff = Date.now() - BUFFER_MS;
        while (this.buffer.length && this.buffer[0].ts < cutoff) this.buffer.shift();
      } catch {
        // игнорируем не-JSON или неожиданные сообщения (например, служебные)
      }
    });

    this.ws.on("close", () => {
      if (this.stopped) return;
      console.log(`[btcPriceFeed] соединение закрыто, переподключение через ${RECONNECT_MS}мс`);
      this.stopPing();
      setTimeout(() => this.connect(), RECONNECT_MS);
    });

    this.ws.on("error", (err) => {
      console.error("[btcPriceFeed] ошибка:", err.message);
    });
  }

  /** RTDS держит соединение живым только если слать текстовый "PING" каждые 5с. */
  private startPing(): void {
    this.stopPing();
    this.pingTimer = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        try {
          this.ws.send("PING");
        } catch {
          // проигнорируем — watchdog всё равно поймает мёртвый сокет по таймауту
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

  /** Следит, что РЕАЛЬНЫЕ TWAP-апдейты приходят. Если тишина слишком долго — считаем сокет мёртвым/зависшим и форсируем переподключение. */
  private startWatchdog(): void {
    this.watchdogTimer = setInterval(() => {
      if (this.stopped) return;
      const idleMs = Date.now() - this.lastDataMessageAt;
      if (idleMs > STALE_TIMEOUT_MS) {
        console.log(`[btcPriceFeed] ⚠️ Нет TWAP-апдейтов ${Math.round(idleMs / 1000)}с — форсируем переподключение.`);
        this.forceReconnect();
      }
    }, WATCHDOG_CHECK_MS);
  }

  private forceReconnect(): void {
    this.lastDataMessageAt = Date.now(); // сброс, чтобы не спамить форс-реконнектом пока идёт переподключение
    this.stopPing();
    try {
      this.ws?.removeAllListeners();
      this.ws?.terminate(); // terminate, а не close — не ждём штатного handshake от уже мёртвого сокета
    } catch (err) {
      console.error("[btcPriceFeed] ошибка при terminate:", (err as Error).message);
    }
    this.connect();
  }

  /** Цена BTC (TWAP), ближайшая по времени к заданному таймстемпу. null — если данных ещё нет. */
  getPriceAt(ts: number): number | null {
    if (this.buffer.length === 0) return null;
    let closest = this.buffer[0];
    for (const s of this.buffer) {
      if (Math.abs(s.ts - ts) < Math.abs(closest.ts - ts)) closest = s;
      if (s.ts > ts) break;
    }
    return closest.price;
  }

  /** Последнее известное значение TWAP. */
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
