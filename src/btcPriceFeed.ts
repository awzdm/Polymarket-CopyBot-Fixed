/**
 * Фид цены BTC с Binance (WebSocket, поток btcusdt@trade).
 * Хранит последние ~10 минут цен с таймстемпами, чтобы можно было
 * найти цену BTC на момент открытия конкретного 5-минутного окна.
 */

import WebSocket from "ws";

const WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade";
const BUFFER_MS = 10 * 60 * 1000;
const RECONNECT_MS = 2000;

interface Sample {
  ts: number;
  price: number;
}

class BtcPriceFeed {
  private ws: WebSocket | null = null;
  private buffer: Sample[] = [];
  private stopped = false;

  start(): void {
    this.connect();
  }

  private connect(): void {
    if (this.stopped) return;
    this.ws = new WebSocket(WS_URL);

    this.ws.on("open", () => {
      console.log("[btcPriceFeed] подключено к Binance (BTCUSDT trade stream)");
    });

    this.ws.on("message", (raw: Buffer) => {
      try {
        const msg = JSON.parse(raw.toString());
        const price = Number(msg.p);
        if (!Number.isFinite(price)) return;
        const ts = Date.now();
        this.buffer.push({ ts, price });
        const cutoff = ts - BUFFER_MS;
        while (this.buffer.length && this.buffer[0].ts < cutoff) this.buffer.shift();
      } catch {
        // игнорируем битые сообщения
      }
    });

    this.ws.on("close", () => {
      if (this.stopped) return;
      console.log(`[btcPriceFeed] соединение закрыто, переподключение через ${RECONNECT_MS}мс`);
      setTimeout(() => this.connect(), RECONNECT_MS);
    });

    this.ws.on("error", (err) => {
      console.error("[btcPriceFeed] ошибка:", err.message);
    });
  }

  /** Цена BTC, ближайшая по времени к заданному таймстемпу. null — если данных ещё нет. */
  getPriceAt(ts: number): number | null {
    if (this.buffer.length === 0) return null;
    let closest = this.buffer[0];
    for (const s of this.buffer) {
      if (Math.abs(s.ts - ts) < Math.abs(closest.ts - ts)) closest = s;
      if (s.ts > ts) break;
    }
    return closest.price;
  }

  /** Последняя известная цена BTC. */
  getLatestPrice(): number | null {
    return this.buffer.length ? this.buffer[this.buffer.length - 1].price : null;
  }

  stop(): void {
    this.stopped = true;
    this.ws?.close();
  }
}

export const btcPriceFeed = new BtcPriceFeed();