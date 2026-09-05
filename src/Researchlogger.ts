/**
 * Исследовательский модуль (не торгует, только собирает статистику).
 *
 * Отслеживает ТОЛЬКО BTC 5-минутные Up/Down рынки на Polymarket.
 *
 * Одновременно симулирует НЕСКОЛЬКО стратегий на одних и тех же живых
 * данных (см. STRATEGIES ниже):
 *   1. "0.98→резолв" — вход по fsdfsdfdfsdкасанию 0.98, держим до официального
 *      резолва, раннего выхода нет вообще.
 *   2. "0.97→0.99"   — вход по касанию 0.97, ранняя победа фиксируется,
 *      если цена долетает до 0.99 ДО резолва.
 *   3. "0.98→0.99"   — вход по касанию 0.98, ранняя победа по 0.99.
 *
 * Логика "сделки" для стратегии с winEarlyLevel:
 *   1. Как только цена токена впервые касается entryLevel — фиксируем
 *      точку входа.
 *   2. С этого момента следим за минимальной ценой этого же токена —
 *      это и есть максимальная просадка сделки.
 *   3. WIN фиксируется досрочно, если цена долетает до winEarlyLevel
 *      ДО официального резолва (если winEarlyLevel = null — этого шага
 *      нет, ждём только резолва).
 *   4. Иначе итог (WIN/LOSS) определяется официальным резолвом через
 *      Gamma API.
 *
 * Раз в REPORT_INTERVAL_MS шлёт промежуточную сводку в Telegram (если
 * настроен), и финальную — при остановке (Ctrl+C).
 *
 * Работает НЕЗАВИСИМО от sniperTrader.ts / fastFlipBot.ts — запускается
 * отдельным процессом, ничего не покупает, только смотрит.
 */

import "dotenv/config";
import { discoverCryptoUpDownMarkets, CryptoUpDownMarket } from "./cryptoMarketDiscovery.js";
import { PriceWatcher, PriceUpdate } from "./priceWatcher.js";
import { createTelegramNotifier } from "./telegram.js";
import { createLogger } from "./logger.js";

// Отслеживаем только BTC и только 5-минутные рынки.
// ВАЖНО: cryptoMarketDiscovery.ts кладёт в поле `coin` название,
// распарсенное из заголовка события (например "Bitcoin Up or Down..."),
// а не тикер — поэтому сравниваем с "Bitcoin", а не с "BTC".
const TARGET_COIN = "Bitcoin";
const TARGET_WINDOW_MINUTES = 5;

const TIMEFRAMES_TO_DISCOVER = [
  { suffixes: ["up-or-down-5m"], minutes: TARGET_WINDOW_MINUTES },
];

interface StrategySpec {
  name: string;
  entryLevel: number;
  // Если null — раннего выхода нет, ждём только официальный резолв.
  winEarlyLevel: number | null;
}

const STRATEGIES: StrategySpec[] = [
  { name: "0.98→резолв", entryLevel: 0.98, winEarlyLevel: null },
  { name: "0.97→0.99", entryLevel: 0.97, winEarlyLevel: 0.99 },
  { name: "0.98→0.99", entryLevel: 0.98, winEarlyLevel: 0.99 },
];

// Границы корзин по времени "сколько секунд оставалось до закрытия окна
// в момент входа в сделку". Последняя корзина — всё, что больше.
const TIME_BUCKETS = [10, 30, 60, 120, 300];

const MARKET_REFRESH_MS = 30 * 1000;
// Наблюдаем каждый рынок с самого его начала — окно наблюдения чуть
// больше длины самого рынка (5-минутке хватит 6 мин запаса).
function observeWindowMs(windowMinutes: number): number {
  return (windowMinutes + 1) * 60 * 1000;
}
// Через сколько секунд после закрытия можно надёжно спросить у Gamma API
// финальный исход (даём время на резолв оракула + запас).
const RESOLVE_CHECK_DELAY_SEC = 180;
// Фразы, на которые бот реагирует и присылает сводку (регистр не важен,
// достаточно чтобы сообщение СОДЕРЖАЛО любую из этих строк).
const REPORT_TRIGGER_PHRASES = ["крипта итог", "crypto report", "/report"];

const GAMMA_HOST = "https://gamma-api.polymarket.com";

interface TradeEvent {
  strategy: string;
  entryLevel: number;
  eventSlug: string;
  side: "Up" | "Down";
  entryTimestamp: number;
  secToCloseAtEntry: number;
  // Минимальная цена, которую видели у ЭТОГО токена с момента входа
  // и до момента, пока сделка не определена как win/loss.
  minPriceSinceEntry: number;
  determined: boolean;
  won: boolean | null;
  wonEarly: boolean; // true, если победа зафиксирована досрочно, а не по резолву
}

interface TokenInfo {
  market: CryptoUpDownMarket;
  side: "Up" | "Down";
}

function buildTokenIndex(markets: CryptoUpDownMarket[]): Map<string, TokenInfo> {
  const idx = new Map<string, TokenInfo>();
  for (const m of markets) {
    idx.set(m.upTokenId, { market: m, side: "Up" });
    idx.set(m.downTokenId, { market: m, side: "Down" });
  }
  return idx;
}

function timeBucketLabel(secToClose: number): string {
  for (let i = 0; i < TIME_BUCKETS.length; i++) {
    if (secToClose <= TIME_BUCKETS[i]) {
      const lo = i === 0 ? 0 : TIME_BUCKETS[i - 1];
      return `${lo}-${TIME_BUCKETS[i]}с`;
    }
  }
  return `>${TIME_BUCKETS[TIME_BUCKETS.length - 1]}с`;
}

function drawdownOf(t: TradeEvent): number {
  // Насколько ниже уровня входа падала цена в худший момент.
  // 0 значит, что цена вообще не опускалась ниже точки входа.
  return Math.max(0, t.entryLevel - t.minPriceSinceEntry);
}

function drawdownBucket(dd: number): string {
  if (dd <= 0) return "0 (без просадки)";
  if (dd <= 0.02) return "0-0.02";
  if (dd <= 0.05) return "0.02-0.05";
  if (dd <= 0.1) return "0.05-0.10";
  if (dd <= 0.2) return "0.10-0.20";
  return ">0.20";
}
const DRAWDOWN_BUCKET_ORDER = ["0 (без просадки)", "0-0.02", "0.02-0.05", "0.05-0.10", "0.10-0.20", ">0.20"];

class ResearchLogger {
  private watcher: PriceWatcher | null = null;
  private tokenIndex = new Map<string, TokenInfo>();
  private lastTokenIds: string[] = [];
  // "strategy:eventSlug:side" -> сделка (если уже зафиксирована точка входа)
  private trades = new Map<string, TradeEvent>();
  private tradesList: TradeEvent[] = [];
  // markets awaiting resolution check: eventSlug -> {closeTimeMs}
  private pendingResolution = new Map<string, { closeTimeMs: number }>();
  private updateCount = 0;

  async refreshMarkets(): Promise<void> {
    let allMarkets: CryptoUpDownMarket[];
    try {
      allMarkets = await discoverCryptoUpDownMarkets(TIMEFRAMES_TO_DISCOVER);
    } catch (err) {
      console.error("[refresh] ошибка:", (err as Error).message);
      return;
    }

    const now = Date.now();
    const markets = allMarkets.filter(
      (m) =>
        m.coin.toUpperCase() === TARGET_COIN.toUpperCase() &&
        m.windowMinutes === TARGET_WINDOW_MINUTES &&
        m.closeTimeMs - now <= observeWindowMs(m.windowMinutes),
    );

    this.tokenIndex = buildTokenIndex(markets);
    const tokenIds = [...this.tokenIndex.keys()].sort();

    for (const m of markets) {
      if (!this.pendingResolution.has(m.eventSlug)) {
        this.pendingResolution.set(m.eventSlug, { closeTimeMs: m.closeTimeMs });
      }
    }

    console.log(
      `[refresh] наблюдаем BTC 5-мин рынков: ${markets.length} (${tokenIds.length} токенов), сделок открыто: ${this.tradesList.length}, ждём резолва: ${this.pendingResolution.size}`,
    );

    const sameAsLastTime =
      tokenIds.length === this.lastTokenIds.length &&
      tokenIds.every((id, i) => id === this.lastTokenIds[i]);
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
    this.updateCount++;
    const info = this.tokenIndex.get(update.tokenId);
    if (!info) return;
    const { market, side } = info;

    const prices = [update.bestBid, update.bestAsk].filter(
      (p): p is number => p !== null,
    );
    if (prices.length === 0) return;
    const price = Math.max(...prices);

    // Один и тот же тик цены прогоняем через ВСЕ стратегии независимо.
    for (const spec of STRATEGIES) {
      const key = `${spec.name}:${market.eventSlug}:${side}`;
      const existing = this.trades.get(key);

      if (existing) {
        if (existing.determined) continue; // итог уже известен
        if (price < existing.minPriceSinceEntry) {
          existing.minPriceSinceEntry = price;
        }
        if (spec.winEarlyLevel !== null && price >= spec.winEarlyLevel) {
          existing.determined = true;
          existing.won = true;
          existing.wonEarly = true;
        }
        continue;
      }

      // Сделки по этой стратегии+токену ещё нет — проверяем точку входа.
      if (price < spec.entryLevel) continue;

      const secToClose = (market.closeTimeMs - Date.now()) / 1000;
      const trade: TradeEvent = {
        strategy: spec.name,
        entryLevel: spec.entryLevel,
        eventSlug: market.eventSlug,
        side,
        entryTimestamp: Date.now(),
        secToCloseAtEntry: secToClose,
        minPriceSinceEntry: price,
        determined: false,
        won: null,
        wonEarly: false,
      };
      this.trades.set(key, trade);
      this.tradesList.push(trade);
    }
  }

  /** Периодически проверяем финальный исход рынков, у которых уже прошло достаточно времени после закрытия. */
  async checkResolutions(): Promise<void> {
    const now = Date.now();
    const toCheck: string[] = [];
    for (const [slug, info] of this.pendingResolution) {
      if (now - info.closeTimeMs >= RESOLVE_CHECK_DELAY_SEC * 1000) {
        toCheck.push(slug);
      }
    }

    for (const slug of toCheck) {
      try {
        const resp = await fetch(`${GAMMA_HOST}/events/slug/${slug}`);
        if (!resp.ok) continue;
        const event = await resp.json();
        const market = (event.markets ?? [])[0];
        if (!market) continue;

        let outcomes: string[];
        let outcomePrices: string[];
        try {
          outcomes = JSON.parse(market.outcomes ?? "[]");
          outcomePrices = JSON.parse(market.outcomePrices ?? "[]");
        } catch {
          continue;
        }
        if (outcomes.length !== 2 || outcomePrices.length !== 2) continue;

        const upIdx = outcomes.findIndex((o) => /^up$/i.test(o.trim()));
        const downIdx = outcomes.findIndex((o) => /^down$/i.test(o.trim()));
        if (upIdx === -1 || downIdx === -1) continue;

        const upPrice = Number(outcomePrices[upIdx]);
        const downPrice = Number(outcomePrices[downIdx]);

        // Не резолвнулся ещё (цены не устаканились на 0/1) — попробуем позже.
        if (upPrice > 0.05 && upPrice < 0.95) continue;

        const winner: "Up" | "Down" = upPrice > downPrice ? "Up" : "Down";

        for (const spec of STRATEGIES) {
          for (const side of ["Up", "Down"] as const) {
            const t = this.trades.get(`${spec.name}:${slug}:${side}`);
            if (t && !t.determined) {
              t.determined = true;
              t.won = side === winner;
              t.wonEarly = false;
            }
          }
        }

        this.pendingResolution.delete(slug);
      } catch (err) {
        console.error(`[resolve] ошибка проверки ${slug}:`, (err as Error).message);
      }
    }
  }

  private buildStrategyReport(spec: StrategySpec): string[] {
    const lines: string[] = [];
    const all = this.tradesList.filter((t) => t.strategy === spec.name);
    const determined = all.filter((t) => t.determined);
    const pending = all.length - determined.length;
    const wins = determined.filter((t) => t.won);
    const losses = determined.filter((t) => !t.won);

    lines.push(`<b>═══ ${spec.name} ═══</b>`);
    lines.push(`Всего сделок: ${all.length} (резолвнуто: ${determined.length}, ждём: ${pending})`);

    if (determined.length > 0) {
      const winRate = ((wins.length / determined.length) * 100).toFixed(1);
      const earlyWins = wins.filter((t) => t.wonEarly).length;
      const earlyNote = spec.winEarlyLevel !== null ? `, из них ранних по ${spec.winEarlyLevel}: ${earlyWins}` : "";
      lines.push(`Win rate: ${wins.length}/${determined.length} (${winRate}%)${earlyNote}`);
    }
    lines.push("");

    const drawdownReport = (label: string, list: TradeEvent[]) => {
      if (list.length === 0) return;
      const dds = list.map(drawdownOf);
      const avg = dds.reduce((a, b) => a + b, 0) / dds.length;
      const worst = Math.max(...dds);
      lines.push(`  <b>Просадка — ${label} (${list.length})</b>`);
      lines.push(`    средняя: ${avg.toFixed(4)}, максимальная: ${worst.toFixed(4)}`);
      const buckets = new Map<string, number>();
      for (const dd of dds) {
        const b = drawdownBucket(dd);
        buckets.set(b, (buckets.get(b) ?? 0) + 1);
      }
      for (const b of DRAWDOWN_BUCKET_ORDER) {
        const c = buckets.get(b);
        if (!c) continue;
        const pct = ((c / list.length) * 100).toFixed(0);
        lines.push(`      ${b}: ${c} (${pct}%)`);
      }
    };

    drawdownReport("выигрышные", wins);
    drawdownReport("проигрышные", losses);
    lines.push("");

    if (determined.length > 0) {
      lines.push(`  <b>Win rate по времени до закрытия на входе</b>`);
      const byBucket = new Map<string, { win: number; total: number }>();
      for (const t of determined) {
        const b = timeBucketLabel(t.secToCloseAtEntry);
        const s = byBucket.get(b) ?? { win: 0, total: 0 };
        s.total++;
        if (t.won) s.win++;
        byBucket.set(b, s);
      }
      for (const [b, s] of [...byBucket.entries()].sort()) {
        const pct = ((s.win / s.total) * 100).toFixed(0);
        lines.push(`    ${b} до закрытия: ${s.win}/${s.total} (${pct}%)`);
      }
    }
    lines.push("");

    return lines;
  }

  buildReport(): string {
    const lines: string[] = [];
    lines.push(`<b>📊 Отчёт BTC 5-мин — сравнение стратегий</b>`);
    lines.push(`Всего сделок по всем стратегиям: ${this.tradesList.length}`);
    lines.push("");

    for (const spec of STRATEGIES) {
      lines.push(...this.buildStrategyReport(spec));
    }

    return lines.join("\n");
  }

  start(): void {
    this.refreshMarkets();
    setInterval(() => this.refreshMarkets(), MARKET_REFRESH_MS);
    setInterval(() => this.checkResolutions(), 30 * 1000);
    setInterval(() => {
      console.log(
        `--- статус: апдейтов цены ${this.updateCount}, сделок ${this.tradesList.length} ---`,
      );
    }, 60 * 1000);
  }
}

/**
 * Слушает входящие сообщения в Telegram (long polling) и отвечает сводкой,
 * когда текст сообщения содержит одну из REPORT_TRIGGER_PHRASES.
 */
async function pollTelegramCommands(
  botToken: string,
  chatId: string,
  telegram: ReturnType<typeof createTelegramNotifier>,
  research: ResearchLogger,
): Promise<void> {
  let offset = 0;
  const apiUrl = `https://api.telegram.org/bot${botToken}/getUpdates`;

  for (;;) {
    try {
      const resp = await fetch(`${apiUrl}?offset=${offset}&timeout=25`);
      if (!resp.ok) {
        await new Promise((r) => setTimeout(r, 5000));
        continue;
      }
      const data = await resp.json();
      for (const update of data.result ?? []) {
        offset = update.update_id + 1;
        const msg = update.message;
        if (!msg?.text || String(msg.chat?.id) !== String(chatId)) continue;

        const text = msg.text.toLowerCase();
        const matched = REPORT_TRIGGER_PHRASES.some((p) => text.includes(p.toLowerCase()));
        if (matched) {
          console.log(`[telegram] Запрос отчёта получен: "${msg.text}"`);
          await telegram?.send(research.buildReport());
        }
      }
    } catch (err) {
      console.error("[telegram poll] ошибка:", (err as Error).message);
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

async function main() {
  console.log("Исследовательский логгер запущен (BTC 5-мин, 3 стратегии, без торговли, только сбор статистики).");
  console.log("Стратегии:", STRATEGIES.map((s) => s.name).join(", "));

  const logger = createLogger(false);
  const telegram = createTelegramNotifier(
    process.env.TELEGRAM_BOT_TOKEN,
    process.env.TELEGRAM_CHAT_ID,
    logger,
  );

  const research = new ResearchLogger();
  research.start();

  if (telegram && process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID) {
    console.log(
      `Telegram включён — напиши боту "${REPORT_TRIGGER_PHRASES[0]}" в любой момент, чтобы получить сводку за всё время работы.`,
    );
    pollTelegramCommands(
      process.env.TELEGRAM_BOT_TOKEN,
      process.env.TELEGRAM_CHAT_ID,
      telegram,
      research,
    );
  } else {
    console.log("Telegram не настроен (нет TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID в .env) — отчёт будет только в консоли.");
  }

  const sendFinal = async () => {
    console.log("\n" + research.buildReport());
    if (telegram) {
      await telegram.send("🌙 Финальный отчёт за ночь:\n\n" + research.buildReport());
    }
    process.exit(0);
  };

  process.on("SIGINT", sendFinal);
  process.on("SIGTERM", sendFinal);
}

main().catch((err) => {
  console.error("Фатальная ошибка:", err);
  process.exit(1);
});