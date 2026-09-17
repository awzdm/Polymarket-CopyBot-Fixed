/**
 * Исследовательский модуль "сетка" (не торгует, только собирает статистику).
 *
 * Симулирует СЕТКУ комбинаций (порог движения монеты × окно входа в
 * секундах) одновременно на живых 5-минутных BTC и ETH рынках — то есть
 * ту же самую логику, что у боевого fastFlip.ts (коридор цены токена
 * 0.97-0.98 + фильтр реального движения монеты от цены открытия окна +
 * окно входа перед закрытием), только сразу для многих комбинаций
 * порог/окно параллельно, чтобы понять, какая комбинация реально лучше
 * ДО того как гонять её живыми деньгами.
 *
 * Пороги:  0.10%, 0.13%, 0.14%, 0.15%, 0.20%, 0.30%
 * Окна:    10с, 30с, 60с, 90с, 120с, 180с, 300с
 * = 42 комбинации на каждую монету (BTC/ETH отдельно), 84 всего.
 *
 * Условие "сделки" (как в боевом боте, держим до резолва, без раннего выхода):
 *   1. Цена токена (Up/Down) в коридоре 0.97-0.98.
 *   2. До закрытия окна осталось ≤ windowSec секунд.
 *   3. Движение цены монеты (Chainlink TWAP через btcPriceFeed/ethPriceFeed)
 *      от цены открытия окна ≥ pctThreshold, в сторону токена.
 * Как только все три условия впервые совпали для конкретной комбинации —
 * фиксируем вход. Итог определяется ТОЛЬКО официальным резолвом Gamma API
 * (никакого раннего выхода/лимитки — так же, как в боевом боте).
 *
 * Раз в 30 мин шлёт КОМПАКТНЫЙ отчёт в Telegram: топ-5 комбинаций по
 * винрейту (с минимальным числом сделок) для BTC и ETH отдельно, плюс
 * "больше всего сделок при винрейте ≥98%". Полную таблицу (все 42×2
 * комбинации) можно запросить отдельной фразой — присылается отдельно,
 * с разбивкой на части (Telegram режет длинные сообщения).
 *
 * Работает НЕЗАВИСИМО от fastFlip.ts — отдельный процесс, ничего не
 * покупает, только смотрит. Свой файл состояния (research-grid-state.json).
 */

import "dotenv/config";
import * as fs from "fs";
import * as path from "path";
import {
  discoverCryptoUpDownMarkets,
  CryptoUpDownMarket,
} from "./cryptoMarketDiscovery.js";
import { PriceWatcher, PriceUpdate } from "./priceWatcher.js";
import { btcPriceFeed } from "./btcPriceFeed.js";
import { ethPriceFeed } from "./ethPriceFeed.js";
import { createTelegramNotifier } from "./telegram.js";
import { createLogger } from "./logger.js";

const STATE_FILE = path.resolve(process.cwd(), "research-grid-state.json");
const AUTOSAVE_INTERVAL_MS = 60 * 1000;

const TARGET_WINDOW_MINUTES = 5; // торгуем только 5-минутные рынки
const TIMEFRAMES_TO_DISCOVER = [{ suffixes: ["up-or-down-5m"], minutes: TARGET_WINDOW_MINUTES }];

const PRICE_LOW = 0.97;
const PRICE_HIGH = 0.98;

// ─── Сетка параметров ───
const THRESHOLDS = [0.001, 0.0013, 0.0014, 0.0015, 0.002, 0.003]; // 0.10% .. 0.30%
const WINDOWS_SEC = [10, 30, 60, 90, 120, 180, 300];

function thresholdLabel(t: number): string {
  return `${(t * 100).toFixed(2)}%`;
}

const MIN_TRADES_FOR_TOP = 15; // минимум сделок, чтобы комбинация попала в топ по винрейту
const HIGH_WINRATE_BAR = 0.98; // порог "хороший винрейт" для строки "больше всего сделок"

interface CoinPriceFeed {
  getPriceAt(ts: number): number | null;
  getLatestPrice(): number | null;
}
const PRICE_FEEDS: Record<string, CoinPriceFeed> = {
  Bitcoin: btcPriceFeed,
  Ethereum: ethPriceFeed,
};
const INCLUDED_COINS = Object.keys(PRICE_FEEDS); // ["Bitcoin", "Ethereum"]

const MARKET_REFRESH_MS = 30 * 1000;
const AUTO_REPORT_INTERVAL_MS = 30 * 60 * 1000;
const RESOLVE_CHECK_DELAY_SEC = 180;
const GAMMA_HOST = "https://gamma-api.polymarket.com";

const COMPACT_REPORT_TRIGGERS = ["крипта итог", "crypto report", "/report"];
const FULL_GRID_TRIGGERS = ["полная таблица", "full grid", "/grid"];

interface ComboTradeEvent {
  pctThreshold: number;
  windowSec: number;
  eventSlug: string;
  coin: string;
  side: "Up" | "Down";
  entryTimestamp: number;
  determined: boolean;
  won: boolean | null;
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

function isIncludedCoin(coin: string): boolean {
  return INCLUDED_COINS.some((c) => c.toUpperCase() === coin.toUpperCase());
}

function observeWindowMs(windowMinutes: number): number {
  return (windowMinutes + 1) * 60 * 1000;
}

class ResearchGridLogger {
  private watcher: PriceWatcher | null = null;
  private tokenIndex = new Map<string, TokenInfo>();
  private lastTokenIds: string[] = [];

  // все комбо-сделки, ключ: `${pctThreshold}_${windowSec}:${eventSlug}:${side}`
  private trades = new Map<string, ComboTradeEvent>();
  private tradesList: ComboTradeEvent[] = [];

  // цена монеты на момент открытия окна, по eventSlug (не зависит от комбинации)
  private openPrices: Map<string, number> = new Map();

  private pendingResolution = new Map<string, { closeTimeMs: number }>();
  private marketsSeen = new Set<string>();
  private updateCount = 0;

  saveState(): void {
    try {
      const data = {
        savedAt: Date.now(),
        tradesList: this.tradesList,
        pendingResolution: [...this.pendingResolution.entries()],
        marketsSeen: [...this.marketsSeen],
        updateCount: this.updateCount,
      };
      const tmpFile = `${STATE_FILE}.tmp`;
      fs.writeFileSync(tmpFile, JSON.stringify(data), "utf-8");
      fs.renameSync(tmpFile, STATE_FILE);
    } catch (err) {
      console.error("[saveState] ошибка сохранения:", (err as Error).message);
    }
  }

  loadState(): void {
    if (!fs.existsSync(STATE_FILE)) {
      console.log("[loadState] файл состояния не найден — начинаем с нуля.");
      return;
    }
    try {
      const raw = fs.readFileSync(STATE_FILE, "utf-8");
      const data = JSON.parse(raw);

      this.tradesList = data.tradesList ?? [];
      this.trades = new Map();
      for (const t of this.tradesList as ComboTradeEvent[]) {
        const key = `${t.pctThreshold}_${t.windowSec}:${t.eventSlug}:${t.side}`;
        this.trades.set(key, t);
      }

      this.pendingResolution = new Map(data.pendingResolution ?? []);
      this.marketsSeen = new Set(data.marketsSeen ?? []);
      this.updateCount = data.updateCount ?? 0;

      const savedAgoSec = data.savedAt ? Math.round((Date.now() - data.savedAt) / 1000) : "?";
      console.log(
        `[loadState] восстановлено комбо-сделок: ${this.tradesList.length}, ждут резолва: ${this.pendingResolution.size} (сохранено ${savedAgoSec}с назад).`,
      );
    } catch (err) {
      console.error("[loadState] ошибка загрузки, начинаем с нуля:", (err as Error).message);
    }
  }

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
        isIncludedCoin(m.coin) &&
        m.windowMinutes === TARGET_WINDOW_MINUTES &&
        m.closeTimeMs - now <= observeWindowMs(m.windowMinutes),
    );

    this.tokenIndex = buildTokenIndex(markets);
    const tokenIds = [...this.tokenIndex.keys()].sort();

    for (const m of markets) {
      this.marketsSeen.add(m.eventSlug);
      if (!this.pendingResolution.has(m.eventSlug)) {
        this.pendingResolution.set(m.eventSlug, { closeTimeMs: m.closeTimeMs });
      }
    }

    // чистим кэш цен открытия для рынков, которых больше нет в наблюдении
    const activeSlugs = new Set(markets.map((m) => m.eventSlug));
    for (const slug of this.openPrices.keys()) {
      if (!activeSlugs.has(slug)) this.openPrices.delete(slug);
    }

    console.log(
      `[refresh] наблюдаем BTC/ETH 5-мин рынков: ${markets.length} (${tokenIds.length} токенов), ` +
        `комбо-сделок открыто: ${this.tradesList.length}, ждём резолва: ${this.pendingResolution.size}`,
    );

    const sameAsLastTime =
      tokenIds.length === this.lastTokenIds.length && tokenIds.every((id, i) => id === this.lastTokenIds[i]);
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

    // Дешёвая проверка первой: коридор цены токена — общий для ВСЕХ комбинаций.
    if (price < PRICE_LOW || price > PRICE_HIGH) return;

    const feed = PRICE_FEEDS[market.coin];
    if (!feed) return;

    const now = Date.now();
    const secToClose = (market.closeTimeMs - now) / 1000;
    if (secToClose < 0) return;

    // Цена монеты на момент открытия окна — считаем один раз на рынок,
    // не на комбинацию (не зависит от порога/окна).
    let openPrice = this.openPrices.get(market.eventSlug);
    if (openPrice === undefined) {
      const openTimeMs = market.closeTimeMs - market.windowMinutes * 60 * 1000;
      const p = feed.getPriceAt(openTimeMs);
      if (p === null) return; // фид ещё не накопил данные на момент открытия окна
      openPrice = p;
      this.openPrices.set(market.eventSlug, openPrice);
    }
    const coinNow = feed.getLatestPrice();
    if (coinNow === null) return;
    const pctMove = (coinNow - openPrice) / openPrice;

    for (const windowSec of WINDOWS_SEC) {
      if (secToClose > windowSec) continue; // ещё не дошли до окна входа этой комбинации

      for (const pctThreshold of THRESHOLDS) {
        const passesMove =
          side === "Up" ? pctMove >= pctThreshold : pctMove <= -pctThreshold;
        if (!passesMove) continue;

        const key = `${pctThreshold}_${windowSec}:${market.eventSlug}:${side}`;
        if (this.trades.has(key)) continue; // уже зафиксирован вход для этой комбинации

        const trade: ComboTradeEvent = {
          pctThreshold,
          windowSec,
          eventSlug: market.eventSlug,
          coin: market.coin,
          side,
          entryTimestamp: now,
          determined: false,
          won: null,
        };
        this.trades.set(key, trade);
        this.tradesList.push(trade);
      }
    }
  }

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
        if (upPrice > 0.05 && upPrice < 0.95) continue;

        const winner: "Up" | "Down" = upPrice > downPrice ? "Up" : "Down";

        for (const t of this.tradesList) {
          if (t.eventSlug === slug && !t.determined) {
            t.determined = true;
            t.won = t.side === winner;
          }
        }

        this.pendingResolution.delete(slug);
        this.saveState();
      } catch (err) {
        console.error(`[resolve] ошибка проверки ${slug}:`, (err as Error).message);
      }
    }
  }

  /** Сводка по одной монете: win/total на каждую комбинацию (только резолвнутые). */
  private gridForCoin(coin: string): Map<string, { win: number; total: number }> {
    const grid = new Map<string, { win: number; total: number }>();
    for (const pctThreshold of THRESHOLDS) {
      for (const windowSec of WINDOWS_SEC) {
        grid.set(`${pctThreshold}_${windowSec}`, { win: 0, total: 0 });
      }
    }
    for (const t of this.tradesList) {
      if (t.coin !== coin || !t.determined) continue;
      const key = `${t.pctThreshold}_${t.windowSec}`;
      const s = grid.get(key);
      if (!s) continue;
      s.total++;
      if (t.won) s.win++;
    }
    return grid;
  }

  /** Компактный отчёт (вариант Б): топ-5 по винрейту + лучшая по частоте при высоком винрейте, для каждой монеты. */
  buildCompactReport(): string {
    const lines: string[] = [];
    lines.push(`<b>📊 Отчёт: сетка порог×окно (BTC/ETH, 5м, коридор ${PRICE_LOW}-${PRICE_HIGH}, держим до резолва)</b>`);
    lines.push(`Уникальных рынков обработано: ${this.marketsSeen.size} | Комбо-сделок всего: ${this.tradesList.length}`);
    lines.push("");

    for (const coin of INCLUDED_COINS) {
      const grid = this.gridForCoin(coin);

      const entries = [...grid.entries()]
        .map(([key, s]) => {
          const [pctStr, winStr] = key.split("_");
          return {
            pctThreshold: Number(pctStr),
            windowSec: Number(winStr),
            win: s.win,
            total: s.total,
            winRate: s.total > 0 ? s.win / s.total : 0,
          };
        })
        .filter((e) => e.total > 0);

      lines.push(`<b>── ${coin}: топ-5 комбинаций по винрейту (мин. ${MIN_TRADES_FOR_TOP} сделок) ──</b>`);
      const top = entries
        .filter((e) => e.total >= MIN_TRADES_FOR_TOP)
        .sort((a, b) => b.winRate - a.winRate || b.total - a.total)
        .slice(0, 5);

      if (top.length === 0) {
        lines.push(`  пока недостаточно данных (нужно ≥${MIN_TRADES_FOR_TOP} сделок на комбинацию)`);
      } else {
        top.forEach((e, i) => {
          lines.push(
            `  ${i + 1}. ${thresholdLabel(e.pctThreshold)} / ${e.windowSec}с — ${(e.winRate * 100).toFixed(0)}% (${e.win}/${e.total})`,
          );
        });
      }
      lines.push("");

      const highWinrateCandidates = entries
        .filter((e) => e.winRate >= HIGH_WINRATE_BAR && e.total >= MIN_TRADES_FOR_TOP)
        .sort((a, b) => b.total - a.total);

      lines.push(`<b>── ${coin}: больше всего сделок при винрейте ≥${(HIGH_WINRATE_BAR * 100).toFixed(0)}% ──</b>`);
      if (highWinrateCandidates.length === 0) {
        lines.push(`  пока нет комбинации с винрейтом ≥${(HIGH_WINRATE_BAR * 100).toFixed(0)}% и ≥${MIN_TRADES_FOR_TOP} сделками`);
      } else {
        const best = highWinrateCandidates[0];
        lines.push(`  ${thresholdLabel(best.pctThreshold)} / ${best.windowSec}с — ${best.total} сделок, ${(best.winRate * 100).toFixed(0)}%`);
      }
      lines.push("");
    }

    lines.push(`Полная таблица по запросу: напиши "${FULL_GRID_TRIGGERS[0]}"`);

    return lines.join("\n");
  }

  /** Полная таблица (вариант А) — все комбинации, по монетам отдельно. */
  buildFullGridReport(): string {
    const lines: string[] = [];
    lines.push(`<b>📊 Полная таблица: сетка порог×окно (BTC/ETH, 5м, коридор ${PRICE_LOW}-${PRICE_HIGH})</b>`);
    lines.push("");

    for (const coin of INCLUDED_COINS) {
      const grid = this.gridForCoin(coin);
      lines.push(`<b>═══ ${coin} ═══</b>`);
      lines.push("");

      const header = "Порог\\Окно  " + WINDOWS_SEC.map((w) => `${w}с`.padEnd(11)).join("");
      lines.push(`<pre>${header}</pre>`);

      for (const pctThreshold of THRESHOLDS) {
        const cells = WINDOWS_SEC.map((windowSec) => {
          const s = grid.get(`${pctThreshold}_${windowSec}`)!;
          const cell = s.total > 0 ? `${s.win}/${s.total} ${(100 * s.win / s.total).toFixed(0)}%` : "—";
          return cell.padEnd(11);
        }).join("");
        lines.push(`<pre>${thresholdLabel(pctThreshold).padEnd(11)}${cells}</pre>`);
      }
      lines.push("");
    }

    return lines.join("\n");
  }

  start(): void {
    this.loadState();
    this.refreshMarkets();
    setInterval(() => this.refreshMarkets(), MARKET_REFRESH_MS);
    setInterval(() => this.checkResolutions(), 30 * 1000);
    setInterval(() => {
      console.log(`--- статус: апдейтов ${this.updateCount}, комбо-сделок ${this.tradesList.length} ---`);
    }, 60 * 1000);
    setInterval(() => this.saveState(), AUTOSAVE_INTERVAL_MS);
  }
}

// Telegram не принимает сообщения длиннее ~4096 символов.
const TELEGRAM_MAX_CHUNK = 3500;

function splitReportIntoChunks(report: string, maxLen: number = TELEGRAM_MAX_CHUNK): string[] {
  const lines = report.split("\n");
  const chunks: string[] = [];
  let current = "";
  for (const line of lines) {
    const candidate = current ? `${current}\n${line}` : line;
    if (candidate.length > maxLen && current) {
      chunks.push(current);
      current = line;
    } else {
      current = candidate;
    }
  }
  if (current) chunks.push(current);
  return chunks.length > 0 ? chunks : [""];
}

async function sendReportToTelegram(
  telegram: ReturnType<typeof createTelegramNotifier>,
  header: string,
  report: string,
): Promise<void> {
  const chunks = splitReportIntoChunks(report);
  for (let i = 0; i < chunks.length; i++) {
    const partLabel = chunks.length > 1 ? ` (часть ${i + 1}/${chunks.length})` : "";
    const text = i === 0 ? `${header}${partLabel}\n\n${chunks[i]}` : `<b>...продолжение${partLabel}</b>\n\n${chunks[i]}`;
    try {
      await telegram?.send(text);
    } catch (err) {
      console.error(`[sendReportToTelegram] ошибка отправки части ${i + 1}/${chunks.length}:`, (err as Error).message);
    }
  }
}

async function pollTelegramCommands(
  botToken: string,
  chatId: string,
  telegram: ReturnType<typeof createTelegramNotifier>,
  research: ResearchGridLogger,
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

        if (COMPACT_REPORT_TRIGGERS.some((p) => text.includes(p.toLowerCase()))) {
          console.log(`[telegram] Запрос компактного отчёта: "${msg.text}"`);
          await sendReportToTelegram(telegram, "<b>📊 Отчёт по запросу</b>", research.buildCompactReport());
          continue;
        }

        if (FULL_GRID_TRIGGERS.some((p) => text.includes(p.toLowerCase()))) {
          console.log(`[telegram] Запрос полной таблицы: "${msg.text}"`);
          await sendReportToTelegram(telegram, "<b>📊 Полная таблица по запросу</b>", research.buildFullGridReport());
          continue;
        }
      }
    } catch (err) {
      console.error("[telegram poll] ошибка:", (err as Error).message);
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

async function main() {
  console.log("Исследовательский логгер СЕТКА запущен (BTC и ETH, 5м, только сбор статистики).");
  console.log("Пороги (%):", THRESHOLDS.map(thresholdLabel).join(", "));
  console.log("Окна входа (с):", WINDOWS_SEC.join(", "));
  console.log(`Комбинаций на монету: ${THRESHOLDS.length * WINDOWS_SEC.length}, монет: ${INCLUDED_COINS.length}`);

  // запускаем оба фида цены (нужны для расчёта % движения)
  btcPriceFeed.start();
  ethPriceFeed.start();

  const logger = createLogger(false);
  const telegram = createTelegramNotifier(process.env.TELEGRAM_BOT_TOKEN, process.env.TELEGRAM_CHAT_ID, logger);

  const research = new ResearchGridLogger();
  research.start();

  if (telegram && process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID) {
    console.log(
      `Telegram включён — напиши "${COMPACT_REPORT_TRIGGERS[0]}" для краткой сводки или "${FULL_GRID_TRIGGERS[0]}" для полной таблицы.`,
    );
    pollTelegramCommands(process.env.TELEGRAM_BOT_TOKEN, process.env.TELEGRAM_CHAT_ID, telegram, research);

    console.log(`Автоотчёт (краткий) включён — каждые ${AUTO_REPORT_INTERVAL_MS / 60000} минут.`);
    setInterval(async () => {
      try {
        await sendReportToTelegram(telegram, "<b>⏰ Автоотчёт (каждые 30 мин)</b>", research.buildCompactReport());
      } catch (err) {
        console.error("[autoReport] ошибка отправки:", (err as Error).message);
      }
    }, AUTO_REPORT_INTERVAL_MS);
  } else {
    console.log("Telegram не настроен — отчёт будет только в консоли.");
  }

  const sendFinal = async () => {
    console.log("\n" + research.buildCompactReport());
    research.saveState();
    if (telegram) {
      await sendReportToTelegram(telegram, "<b>🌙 Финальный отчёт за ночь</b>", research.buildCompactReport());
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
