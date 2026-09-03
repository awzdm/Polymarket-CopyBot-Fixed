/**
 * FastFlip Parser — сбор статистики по касанию 0.98, БЕЗ реальных сделок.
 * Работает параллельно основному боту, ничего не трогает и не торгует.
 *
 * Логика:
 *  1. Следим только за BTC 5-мин рынками.
 *  2. Первый токен (Up или Down), который коснулся TOUCH_PRICE — фиксируем
 *     как "касание" (второй токен того же рынка игнорируем).
 *  3. После касания следим за минимальной ценой вплоть до закрытия рынка.
 *  4. На закрытии: finalPrice >= 0.999 -> WIN, иначе -> LOSS.
 *  5. Каждая запись пишется в data/fastflip-parser-log.jsonl (копится вечно).
 *  6. Раз в SUMMARY_INTERVAL_MS шлём сводку в Telegram.
 */

import "dotenv/config";
import { discoverCryptoUpDownMarkets, CryptoUpDownMarket } from "./cryptoMarketDiscovery.js";
import { PriceWatcher, PriceUpdate } from "./priceWatcher.js";
import { createTelegramNotifier } from "./telegram.js";
import { createLogger } from "./logger.js";
import fs from "fs";
import path from "path";

const COIN = "Bitcoin";
const TOUCH_PRICE = Number(process.env.PARSER_TOUCH_PRICE ?? "0.98");
const MARKET_REFRESH_MS = 30 * 1000;
const OBSERVE_WINDOW_MS = 6 * 60 * 1000;
const CLOSE_GRACE_MS = 15 * 1000; // ждём чуть после закрытия чтобы поймать финальную цену
const SUMMARY_INTERVAL_MS = 2 * 60 * 60 * 1000; // каждые 2 часа
const DATA_FILE = path.join(process.cwd(), "data", "fastflip-parser-log.jsonl");

interface TokenInfo {
  market: CryptoUpDownMarket;
  side: "Up" | "Down";
}

interface TouchRecord {
  eventSlug: string;
  coin: string;
  side: "Up" | "Down";
  windowStartMs: number;
  closeTimeMs: number;
  touchTimeMs: number;
  touchSecFromStart: number;
  minPriceAfterTouch: number;
  finalPrice: number | null;
  outcome: "WIN" | "LOSS" | "UNKNOWN";
  createdAt: string;
}

function ensureDataDir() {
  const dir = path.dirname(DATA_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function appendRecord(rec: TouchRecord) {
  ensureDataDir();
  fs.appendFileSync(DATA_FILE, JSON.stringify(rec) + "\n");
}

class TrackedTouch {
  minPriceAfterTouch: number;
  finalPrice: number | null = null;

  constructor(
    public market: CryptoUpDownMarket,
    public side: "Up" | "Down",
    public touchTimeMs: number,
    public touchPrice: number,
  ) {
    this.minPriceAfterTouch = touchPrice;
  }
}

class FastFlipParser {
  private watcher: PriceWatcher | null = null;
  private tokenIndex = new Map<string, TokenInfo>();
  private lastTokenIds: string[] = [];

  private touchedMarkets = new Set<string>(); // eventSlug -> уже кто-то коснулся 0.98
  private tracked = new Map<string, TrackedTouch>(); // tokenId -> отслеживание до закрытия

  private sessionRecords: TouchRecord[] = []; // копится между сводками, потом чистится

  constructor(private telegram: ReturnType<typeof createTelegramNotifier>) {}

  async refreshMarkets(): Promise<void> {
    let allMarkets: CryptoUpDownMarket[];
    try {
      allMarkets = await discoverCryptoUpDownMarkets([{ suffixes: ["up-or-down-5m"], minutes: 5 }]);
    } catch (err) {
      console.error("[parser][refresh] ошибка:", (err as Error).message);
      return;
    }

    const now = Date.now();
    const markets = allMarkets.filter(
      (m) => m.coin === COIN && m.closeTimeMs - now <= OBSERVE_WINDOW_MS,
    );

    const idx = new Map<string, TokenInfo>();
    for (const m of markets) {
      idx.set(m.upTokenId, { market: m, side: "Up" });
      idx.set(m.downTokenId, { market: m, side: "Down" });
    }
    this.tokenIndex = idx;

    const tokenIds = [...this.tokenIndex.keys()].sort();
    const sameAsLastTime =
      tokenIds.length === this.lastTokenIds.length &&
      tokenIds.every((id, i) => id === this.lastTokenIds[i]);

    console.log(`[parser][refresh] рынков BTC: ${markets.length}, отслеживается касаний: ${this.tracked.size}`);

    if (sameAsLastTime && this.watcher) return;
    this.lastTokenIds = tokenIds;

    this.watcher?.stop();
    if (tokenIds.length === 0) {
      this.watcher = null;
      return;
    }
    this.watcher = new PriceWatcher(tokenIds, (u) => this.onPriceUpdate(u));
    this.watcher.start();
  }

  private onPriceUpdate(update: PriceUpdate): void {
    const info = this.tokenIndex.get(update.tokenId);
    if (!info) return;
    const { market, side } = info;
    const price = update.bestBid ?? update.bestAsk;
    if (price === null) return;

    const tracked = this.tracked.get(update.tokenId);
    if (tracked) {
      if (price < tracked.minPriceAfterTouch) tracked.minPriceAfterTouch = price;
      tracked.finalPrice = price;
      return;
    }

    if (this.touchedMarkets.has(market.eventSlug)) return;
    if (price < TOUCH_PRICE) return;

    this.touchedMarkets.add(market.eventSlug);
    const windowStartMs = market.closeTimeMs - 5 * 60 * 1000;
    const t = new TrackedTouch(market, side, Date.now(), price);
    t.finalPrice = price;
    this.tracked.set(update.tokenId, t);

    console.log(
      `[parser] 🎯 КАСАНИЕ ${TOUCH_PRICE}: [${market.coin}] "${market.title}" сторона ${side} цена ${price} (через ${((Date.now() - windowStartMs) / 1000).toFixed(0)}с от старта окна)`,
    );
  }

  private finalizeClosedMarkets(): void {
    const now = Date.now();
    for (const [tokenId, t] of this.tracked) {
      if (now < t.market.closeTimeMs + CLOSE_GRACE_MS) continue;

      const windowStartMs = t.market.closeTimeMs - 5 * 60 * 1000;
      const finalPrice = t.finalPrice;
      const outcome: TouchRecord["outcome"] =
        finalPrice === null ? "UNKNOWN" : finalPrice >= 0.999 ? "WIN" : "LOSS";

      const rec: TouchRecord = {
        eventSlug: t.market.eventSlug,
        coin: t.market.coin,
        side: t.side,
        windowStartMs,
        closeTimeMs: t.market.closeTimeMs,
        touchTimeMs: t.touchTimeMs,
        touchSecFromStart: Math.round((t.touchTimeMs - windowStartMs) / 1000),
        minPriceAfterTouch: Number(t.minPriceAfterTouch.toFixed(4)),
        finalPrice: finalPrice === null ? null : Number(finalPrice.toFixed(4)),
        outcome,
        createdAt: new Date().toISOString(),
      };

      appendRecord(rec);
      this.sessionRecords.push(rec);
      this.tracked.delete(tokenId);

      console.log(
        `[parser] ✅ Записано: ${rec.eventSlug} ${rec.side} touch@${rec.touchSecFromStart}с min=${rec.minPriceAfterTouch} final=${rec.finalPrice} -> ${rec.outcome}`,
      );
    }
  }

  private buildSummary(records: TouchRecord[]): string {
    if (records.length === 0) return "📊 За этот период новых касаний 0.98 не было.";

    const wins = records.filter((r) => r.outcome === "WIN");
    const losses = records.filter((r) => r.outcome === "LOSS");
    const winRate = ((wins.length / records.length) * 100).toFixed(1);

    const avgMinWin =
      wins.length > 0
        ? (wins.reduce((s, r) => s + r.minPriceAfterTouch, 0) / wins.length).toFixed(4)
        : "—";
    const worstMinWin =
      wins.length > 0 ? Math.min(...wins.map((r) => r.minPriceAfterTouch)).toFixed(4) : "—";

    const avgTouchSec = (records.reduce((s, r) => s + r.touchSecFromStart, 0) / records.length).toFixed(0);

    const dips = [0.97, 0.95, 0.93, 0.9, 0.85, 0.8];
    const dipLines = dips
      .map((d) => {
        const knockedOut = wins.filter((r) => r.minPriceAfterTouch < d).length;
        const pct = wins.length > 0 ? ((knockedOut / wins.length) * 100).toFixed(1) : "0.0";
        return `   стоп ${d}: выбило бы ${knockedOut}/${wins.length} побед (${pct}%)`;
      })
      .join("\n");

    return (
      `📊 Статистика FastFlip Parser (BTC)\n` +
      `Всего касаний 0.98: ${records.length}\n` +
      `WIN: ${wins.length} | LOSS: ${losses.length} | Винрейт: ${winRate}%\n` +
      `Среднее время касания от старта окна: ${avgTouchSec}с\n\n` +
      `Среди победивших сделок:\n` +
      `  Средняя минимальная просадка: ${avgMinWin}\n` +
      `  Худшая минимальная просадка: ${worstMinWin}\n\n` +
      `Если бы стоял стоп-лосс на разных уровнях (сколько побед он бы выбил зря):\n${dipLines}`
    );
  }

  async sendPeriodicSummary(): Promise<void> {
    const summary = this.buildSummary(this.sessionRecords);
    console.log(`\n${summary}\n`);
    if (this.telegram) await this.telegram.send(summary);
    this.sessionRecords = [];
  }

  start(): void {
    this.refreshMarkets();
    setInterval(() => this.refreshMarkets(), MARKET_REFRESH_MS);
    setInterval(() => this.finalizeClosedMarkets(), 5 * 1000);
    setInterval(() => this.sendPeriodicSummary(), SUMMARY_INTERVAL_MS);
  }
}

async function main() {
  console.log("Режим: PARSER (сбор статистики, без реальных сделок)");
  console.log(`Актив: BTC only | Порог касания: ${TOUCH_PRICE}`);

  const logger = createLogger(false);
  const telegram = createTelegramNotifier(process.env.TELEGRAM_BOT_TOKEN, process.env.TELEGRAM_CHAT_ID, logger);

  const parser = new FastFlipParser(telegram);
  parser.start();

  if (telegram) {
    await telegram.send("🟢 FastFlip Parser запущен. Собираю статистику по BTC (без сделок).");
  }
}

main().catch((err) => {
  console.error("Фатальная ошибка:", err);
  process.exit(1);
});
