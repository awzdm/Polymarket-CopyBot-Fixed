/**
 * Модуль 2: слежение за ценами в реальном времени через CLOB WebSocket.
 * Подписывается на список токенов, вызывает callback при каждом изменении цены.
 *
 * ВАЖНО: у websocket-соединений бывает "тихая смерть" — сеть/прокси между
 * нами и биржей может оборвать сокет без штатного события "close" (например
 * NAT/прокси-таймаут на простое). Формально сокет выглядит открытым, но
 * сообщения больше не приходят. Раньше это маскировалось частой сменой
 * набора токенов (при смене список пересоздавался, что попутно чинило
 * зависшее соединение). Теперь, когда список токенов может не меняться
 * подолгу (например, целый час), нужен явный watchdog по времени
 * последнего полученного сообщения.
 */

import WebSocket from "ws";

const WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const STALE_TIMEOUT_MS = 45 * 1000; // если так долго нет сообщений — считаем сокет мёртвым
const WATCHDOG_CHECK_MS = 10 * 1000;
const PING_INTERVAL_MS = 20 * 1000;

export interface PriceUpdate {
  tokenId: string;
  bestAsk: number | null;
  bestBid: number | null;
  timestamp: number;
}

export class PriceWatcher {
  private ws: WebSocket | null = null;
  private tokenIds: string[];
  private onUpdate: (update: PriceUpdate) => void;
  private stopped = false;
  private lastMessageAt = Date.now();
  private watchdogTimer: NodeJS.Timeout | null = null;
  private pingTimer: NodeJS.Timeout | null = null;

  constructor(tokenIds: string[], onUpdate: (update: PriceUpdate) => void) {
    this.tokenIds = tokenIds;
    this.onUpdate = onUpdate;
  }

  start(): void {
    this.connect(1000);
    this.startWatchdog();
  }

  private connect(backoffMs: number): void {
    if (this.stopped) return;

    this.lastMessageAt = Date.now(); // не считаем зависшим сразу после (пере)подключения
    this.ws = new WebSocket(WS_URL);

    this.ws.on("open", () => {
      console.log(`[priceWatcher] подключено, подписка на ${this.tokenIds.length} токенов`);
      this.lastMessageAt = Date.now();
      this.ws!.send(JSON.stringify({ type: "market", assets_ids: this.tokenIds }));
      this.startPing();
    });

    this.ws.on("message", (raw: Buffer) => {
      this.lastMessageAt = Date.now();
      this.handleMessage(raw.toString());
    });

    this.ws.on("pong", () => {
      this.lastMessageAt = Date.now();
    });

    this.ws.on("close", () => {
      if (this.stopped) return;
      console.log(`[priceWatcher] соединение закрыто, переподключение через ${backoffMs}мс`);
      this.stopPing();
      setTimeout(() => this.connect(Math.min(backoffMs * 2, 30000)), backoffMs);
    });

    this.ws.on("error", (err) => {
      console.error("[priceWatcher] ошибка:", err.message);
    });
  }

  /** Периодически шлём ping — многие прокси/NAT держат соединение живым только пока идёт трафик. */
  private startPing(): void {
    this.stopPing();
    this.pingTimer = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        try {
          this.ws.ping();
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

  /** Следит за тем, что сообщения (или pong) реально приходят. Если тишина слишком долго — считаем сокет мёртвым и форсируем переподключение. */
  private startWatchdog(): void {
    this.watchdogTimer = setInterval(() => {
      if (this.stopped) return;
      const idleMs = Date.now() - this.lastMessageAt;
      if (idleMs > STALE_TIMEOUT_MS) {
        console.log(`[priceWatcher] ⚠️ Нет сообщений ${Math.round(idleMs / 1000)}с — соединение считаем мёртвым, форсируем переподключение.`);
        this.forceReconnect();
      }
    }, WATCHDOG_CHECK_MS);
  }

  private forceReconnect(): void {
    this.lastMessageAt = Date.now(); // сброс, чтобы не спамить форс-реконнектом пока идёт переподключение
    this.stopPing();
    try {
      this.ws?.removeAllListeners();
      this.ws?.terminate(); // terminate, а не close — не ждём штатного handshake от уже мёртвого сокета
    } catch (err) {
      console.error("[priceWatcher] ошибка при terminate:", (err as Error).message);
    }
    this.connect(1000);
  }

  private handleMessage(raw: string): void {
    let msg: any;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }
    const events = Array.isArray(msg) ? msg : [msg];
    for (const event of events) {
      const tokenId = event.asset_id;
      if (!tokenId) continue;

      const asks = (event.asks ?? []).map((a: any) => Number(a.price)).filter((p: number) => Number.isFinite(p));
      const bids = (event.bids ?? []).map((b: any) => Number(b.price)).filter((p: number) => Number.isFinite(p));

      this.onUpdate({
        tokenId,
        bestAsk: asks.length ? Math.min(...asks) : null,
        bestBid: bids.length ? Math.max(...bids) : null,
        timestamp: Date.now(),
      });
    }
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

// Тестовый запуск: npx tsx src/priceWatcher.ts
if (import.meta.url === `file://${process.argv[1]}`) {
  import("./marketDiscovery.js").then(async ({ discoverWeatherMarkets }) => {
    const markets = await discoverWeatherMarkets();
    // Берём 40 рынков вместо 3 — больше шансов поймать реальное движение цены
    const testMarkets = markets.slice(0, 40);
    const tokenIds = testMarkets.flatMap((m) => m.bins.flatMap((b) => [b.yesTokenId, b.noTokenId]));
    console.log(`Тестируем на ${testMarkets.length} рынках, ${tokenIds.length} токенов`);

    let updateCount = 0;
    const watcher = new PriceWatcher(tokenIds, (update) => {
      updateCount++;
      console.log(`[${new Date(update.timestamp).toISOString()}] #${updateCount} token=${update.tokenId.slice(0, 12)}... ask=${update.bestAsk} bid=${update.bestBid}`);
    });
    watcher.start();

    setInterval(() => {
      console.log(`--- всего сообщений получено: ${updateCount} ---`);
    }, 15000);
  });
}