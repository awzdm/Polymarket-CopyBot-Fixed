/**
 * Исследовательский модуль (не торгует, только собирает статистику).
 *
 * Мониторит ВСЕ активные крипто up/down рынки (5-мин, 15-мин, час, 4 часа,
 * день) с самого их появления.
 * стороны каждого рынка отслеживает момент первого пересечения уровней
 * цены 0.99 / 0.98 / 0.97 / 0.96 / 0.95, и сколько секунд на тот момент
 * оставалось до закрытия окна.
 *
 * После резолва рынка (через Gamma API) узнаём, какая сторона реально
 * победила, и помечаем каждое записанное пересечение: "held" (удержалось,
 * сторона победила) или "reversed" (развернулось, сторона проиграла).
 *
 * Раз в REPORT_INTERVAL_MS шлёт промежуточную сводку в Telegram (если
 * настроен), и финальную — при остановке (Ctrl+C).
 *
 * Работает НЕЗАВИСИМО от sniperTrader.ts — запускается отдельным
 * процессом, ничего не покупает, только смотрит.
 */

import "dotenv/config";
import { discoverCryptoUpDownMarkets, CryptoUpDownMarket, ALL_TIMEFRAMES } from "./cryptoMarketDiscovery.js";
import { PriceWatcher, PriceUpdate } from "./priceWatcher.js";
import { createTelegramNotifier } from "./telegram.js";
import { createLogger } from "./logger.js";

const LEVELS = [0.99, 0.98, 0.97, 0.96, 0.95];
// Границы корзин по времени "сколько секунд оставалось до закрытия окна
// в момент пересечения уровня". Последняя корзина — всё, что больше.
const TIME_BUCKETS = [10, 30, 60, 120, 300];
const MARKET_REFRESH_MS = 30 * 1000;
// В отличие от торгового бота (3 мин), тут наблюдаем ВЕСЬ активный
// 5-минутный рынок с момента его появления — окно берём с запасом чуть
// больше длины самого рынка.
// Наблюдаем каждый рынок с самого его начала — окно наблюдения зависит от
// длины самого рынка (5-минутке хватит 6 мин запаса, дневному — почти сутки).
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

interface CrossingEvent {
  coin: string;
  eventSlug: string;
  windowMinutes: number;
  side: "Up" | "Down";
  level: number;
  secToCloseAtCross: number;
  timestamp: number;
  resolved: boolean;
  won: boolean | null; // null пока не узнали исход
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

class ResearchLogger {
  private watcher: PriceWatcher | null = null;
  private tokenIndex = new Map<string, TokenInfo>();
  private lastTokenIds: string[] = [];
  // eventSlug:side:level -> уже записано (не дублируем)
  private seenCrossings = new Set<string>();
  private crossings: CrossingEvent[] = [];
  // markets awaiting resolution check: eventSlug -> {closeTimeMs, coin}
  private pendingResolution = new Map<string, { closeTimeMs: number; coin: string }>();
  private updateCount = 0;

  async refreshMarkets(): Promise<void> {
    let allMarkets: CryptoUpDownMarket[];
    try {
      allMarkets = await discoverCryptoUpDownMarkets(ALL_TIMEFRAMES);
    } catch (err) {
      console.error("[refresh] ошибка:", (err as Error).message);
      return;
    }

    // Наблюдаем ВСЕ таймфреймы (5м/15м/час/4ч/день), каждый со своим
    // окном наблюдения от начала своего же периода.
    const now = Date.now();
    const markets = allMarkets.filter((m) => m.closeTimeMs - now <= observeWindowMs(m.windowMinutes));

    this.tokenIndex = buildTokenIndex(markets);
    const tokenIds = [...this.tokenIndex.keys()].sort();

    for (const m of markets) {
      if (!this.pendingResolution.has(m.eventSlug)) {
        this.pendingResolution.set(m.eventSlug, { closeTimeMs: m.closeTimeMs, coin: m.coin });
      }
    }

    console.log(
      `[refresh] наблюдаем рынков: ${markets.length} (${tokenIds.length} токенов), пересечений записано: ${this.crossings.length}, ждём резолва: ${this.pendingResolution.size}`,
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

    const price = update.bestBid ?? update.bestAsk;
    if (price === null) return;

    const secToClose = (market.closeTimeMs - Date.now()) / 1000;

    for (const level of LEVELS) {
      if (price < level) continue;
      const key = `${market.eventSlug}:${side}:${level}`;
      if (this.seenCrossings.has(key)) continue;
      this.seenCrossings.add(key);

      this.crossings.push({
        coin: market.coin,
        eventSlug: market.eventSlug,
        windowMinutes: market.windowMinutes,
        side,
        level,
        secToCloseAtCross: secToClose,
        timestamp: Date.now(),
        resolved: false,
        won: null,
      });
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

        // outcomePrices обычно ["1", "0"] или ["0", "1"] после резолва —
        // порядок соответствует outcomes (["Up","Down"]).
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

        for (const c of this.crossings) {
          if (c.eventSlug === slug && !c.resolved) {
            c.resolved = true;
            c.won = c.side === winner;
          }
        }

        this.pendingResolution.delete(slug);
      } catch (err) {
        console.error(`[resolve] ошибка проверки ${slug}:`, (err as Error).message);
      }
    }
  }

  buildReport(): string {
    const resolved = this.crossings.filter((c) => c.resolved);
    const pending = this.crossings.length - resolved.length;

    type Key = string; // "windowMinutes|level|bucket"
    const stats = new Map<Key, { held: number; reversed: number }>();

    for (const c of resolved) {
      const bucket = timeBucketLabel(c.secToCloseAtCross);
      const key = `${c.windowMinutes}|${c.level}|${bucket}`;
      const s = stats.get(key) ?? { held: 0, reversed: 0 };
      if (c.won) s.held++;
      else s.reversed++;
      stats.set(key, s);
    }

    const timeframeLabel = (m: number) =>
      m === 5 ? "5 минут" : m === 15 ? "15 минут" : m === 60 ? "1 час" : m === 240 ? "4 часа" : m === 1440 ? "1 день" : `${m} мин`;

    const timeframesPresent = [...new Set(resolved.map((c) => c.windowMinutes))].sort((a, b) => a - b);

    const lines: string[] = [];
    lines.push(`<b>📊 Отчёт по уровням входа (крипто рынки, все таймфреймы)</b>`);
    lines.push(`Всего пересечений записано: ${this.crossings.length} (резолвнуто: ${resolved.length}, ждём: ${pending})`);
    lines.push("");

    // Сравнение монет — по всем данным сразу (все таймфреймы/уровни/бакеты
    // вместе), чтобы сразу видеть, какая монета в среднем надёжнее.
    const coinStats = new Map<string, { held: number; total: number }>();
    for (const c of resolved) {
      const s = coinStats.get(c.coin) ?? { held: 0, total: 0 };
      s.total++;
      if (c.won) s.held++;
      coinStats.set(c.coin, s);
    }
    if (coinStats.size > 0) {
      lines.push(`<b>Сравнение монет (по всем данным)</b>`);
      const ranked = [...coinStats.entries()].sort((a, b) => b[1].held / b[1].total - a[1].held / a[1].total);
      for (const [coin, s] of ranked) {
        const pct = ((s.held / s.total) * 100).toFixed(1);
        lines.push(`  ${coin}: ${s.held}/${s.total} удержалось (${pct}%)`);
      }
      lines.push("");
    }

    if (timeframesPresent.length === 0) {
      lines.push("Пока нет резолвнутых данных.");
      return lines.join("\n");
    }

    for (const windowMinutes of timeframesPresent) {
      lines.push(`<b>═══ ${timeframeLabel(windowMinutes)} ═══</b>`);
      for (const level of LEVELS) {
        let anyForLevel = false;
        const levelLines: string[] = [];
        for (let i = 0; i <= TIME_BUCKETS.length; i++) {
          const bucket =
            i < TIME_BUCKETS.length
              ? `${i === 0 ? 0 : TIME_BUCKETS[i - 1]}-${TIME_BUCKETS[i]}с`
              : `>${TIME_BUCKETS[TIME_BUCKETS.length - 1]}с`;
          const s = stats.get(`${windowMinutes}|${level}|${bucket}`);
          if (!s || s.held + s.reversed === 0) continue;
          anyForLevel = true;
          const total = s.held + s.reversed;
          const pct = ((s.held / total) * 100).toFixed(0);
          levelLines.push(`    ${bucket} до закрытия: ${s.held}/${total} удержалось (${pct}%)`);
        }
        if (anyForLevel) {
          lines.push(`  Уровень ${level}`);
          lines.push(...levelLines);
        }
      }
      lines.push("");
    }

    return lines.join("\n");
  }

  start(): void {
    this.refreshMarkets();
    setInterval(() => this.refreshMarkets(), MARKET_REFRESH_MS);
    setInterval(() => this.checkResolutions(), 30 * 1000);
    setInterval(() => {
      console.log(
        `--- статус: апдейтов цены ${this.updateCount}, пересечений ${this.crossings.length} ---`,
      );
    }, 60 * 1000);
  }

  getCrossingsCount(): number {
    return this.crossings.length;
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
  console.log("Исследовательский логгер запущен (без торговли, только сбор статистики).");

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