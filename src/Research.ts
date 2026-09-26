/**
 * Исследовательский модуль "сетка" (не торгует, только собирает статистику).
 *
 * v2 (относительно предыдущей версии): добавлена ВТОРАЯ сетка — АДАПТИВНАЯ,
 * где порог движения не фиксированное число (0.13%, 0.5% и т.д.), а
 * multiplier × recentVol, где recentVol — "типичное" 5-минутное движение
 * этой монеты за последний час (см. volatilityTracker.ts). В тихий рынок
 * порог сам подстраивается ниже, в дёрганый — выше. Обе сетки (fixed и
 * adaptive) считаются ПАРАЛЛЕЛЬНО на одних и тех же живых данных, чтобы
 * потом сравнить их отчётами напрямую.
 *
 * Также добавлен ПАССИВНЫЙ (ничего не решающий) лог "замедления тейпа":
 * для каждого зафиксированного входа (в любой сетке) сохраняем движение
 * монеты за последние 10с ДО входа и за 10с ДО этого — просто чтобы потом
 * посмотреть, коррелирует ли замедление движения перед входом с винрейтом.
 * Не влияет на критерии входа ни в одной из сеток.
 *
 * Симулирует всё это одновременно на живых 5-минутных крипто-рынках — та
 * же самая логика, что у боевого fastFlip.ts (коридор цены токена
 * 0.97-0.98 + фильтр реального движения монеты от цены открытия окна +
 * окно входа перед закрытием), только сразу для многих комбинаций
 * порог/окно параллельно, чтобы понять, какая комбинация реально лучше
 * ДО того как гонять её живыми деньгами.
 *
 * Fixed-пороги:  0.10%, 0.13%, 0.14%, 0.15%, 0.20%, 0.30%
 * Adaptive-мультипликаторы: 0.3x, 0.5x, 0.7x, 1.0x, 1.5x, 2.0x (× recentVol)
 * Окна:    10с, 30с, 60с, 90с, 120с, 180с, 300с
 * Монеты: BTC, ETH, SOL, XRP, DOGE.
 *
 * ВАЖНО: fixed-пороги ОДНИ И ТЕ ЖЕ для всех монет — намеренно, чтобы дать
 * сетке параметров самой показать, какой порог реально работает для
 * каждой монеты (отчёт разбит по монетам). Adaptive-мультипликаторы тоже
 * одни и те же для всех монет — потому что они уже нормированы на
 * волатильность конкретной монеты через recentVol.
 *
 * Условие "сделки" (как в боевом боте, держим до резолва, без раннего выхода):
 *   1. Цена токена (Up/Down) в коридоре 0.97-0.98.
 *   2. До закрытия окна осталось ≤ windowSec секунд.
 *   3. Движение цены монеты (Chainlink TWAP через btcPriceFeed/ethPriceFeed/...)
 *      от цены открытия окна ≥ порога (fixed-число ИЛИ multiplier×recentVol),
 *      в сторону токена.
 * Как только все три условия впервые совпали для конкретной комбинации —
 * фиксируем вход. Итог определяется ТОЛЬКО официальным резолвом Gamma API
 * (никакого раннего выхода/лимитки — так же, как в боевом боте).
 *
 * Раз в 30 мин шлёт КОМПАКТНЫЙ отчёт в Telegram: топ-5 комбинаций по
 * винрейту (с минимальным числом сделок) для каждой монеты, отдельно для
 * fixed и adaptive сеток. Полную таблицу и отчёт по объёму/замедлению
 * можно запросить отдельной фразой.
 *
 * ДОПОЛНИТЕЛЬНО: для каждого входа фиксируется дисбаланс объёма
 * купли/продажи (реальные исполненные сделки на токене, не цена) за
 * последние 90с и 120с до момента входа — через tradeFlowTracker.ts,
 * публичный канал Polymarket (last_trade_price). Отчёт по объёму —
 * отдельной фразой ("объём").
 *
 * Работает НЕЗАВИСИМО от fastFlip.ts — отдельный процесс, ничего не
 * покупает, только смотрит. Свой файл состояния (research-grid-state.json).
 * Старый файл состояния (без mode/adaptive/tickDecel полей) загружается
 * без потерь — недостающие поля бэкфиллятся при загрузке.
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
import { solPriceFeed } from "./solPriceFeed.js";
import { xrpPriceFeed } from "./xrpPriceFeed.js";
import { dogePriceFeed } from "./dogePriceFeed.js";
import { tradeFlowTracker } from "./tradeFlowTracker.js";
import { VolatilityTracker } from "./volatilityTracker.js";
import { createTelegramNotifier } from "./telegram.js";
import { createLogger } from "./logger.js";

const STATE_FILE = path.resolve(process.cwd(), "research-grid-state.json");
const AUTOSAVE_INTERVAL_MS = 60 * 1000;

const TARGET_WINDOW_MINUTES = 5; // торгуем только 5-минутные рынки
const TIMEFRAMES_TO_DISCOVER = [{ suffixes: ["up-or-down-5m"], minutes: TARGET_WINDOW_MINUTES }];

const PRICE_LOW = 0.97;
const PRICE_HIGH = 0.98;

// ─── Сетка параметров: FIXED (как раньше) ───
const THRESHOLDS = [0.001, 0.0013, 0.0014, 0.0015, 0.002, 0.003]; // 0.10% .. 0.30%

// ─── Сетка параметров: ADAPTIVE (новое) ───
// Порог = multiplier × recentVol (recentVol — типичное 5-мин движение монеты за последний час).
const VOL_MULTIPLIERS = [0.3, 0.5, 0.7, 1.0, 1.5, 2.0];

// ─── Общее для обеих сеток ───
const WINDOWS_SEC = [10, 30, 60, 90, 120, 180, 300];

// Пассивный лог "замедления тейпа" — окна замера движения ДО входа.
const TICK_DECEL_WINDOW_MS = 10 * 1000;

function thresholdLabel(t: number): string {
  return `${(t * 100).toFixed(2)}%`;
}
function multiplierLabel(m: number): string {
  return `${m.toFixed(1)}x`;
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
  Solana: solPriceFeed,
  XRP: xrpPriceFeed,
  Dogecoin: dogePriceFeed,
};
const INCLUDED_COINS = Object.keys(PRICE_FEEDS); // ["Bitcoin", "Ethereum", "Solana", "XRP", "Dogecoin"]

const volTracker = new VolatilityTracker(PRICE_FEEDS);

const MARKET_REFRESH_MS = 30 * 1000;
const AUTO_REPORT_INTERVAL_MS = 30 * 60 * 1000;
const RESOLVE_CHECK_DELAY_SEC = 180;
const GAMMA_HOST = "https://gamma-api.polymarket.com";

const COMPACT_REPORT_TRIGGERS = ["крипта итог", "crypto report", "/report"];
const FULL_GRID_TRIGGERS = ["полная таблица", "full grid", "/grid"];
const ADAPTIVE_REPORT_TRIGGERS = ["адаптив", "adaptive report", "/adaptive"];
const ADAPTIVE_FULL_GRID_TRIGGERS = ["полная адаптив", "full adaptive grid", "/gridadaptive"];
const VOLUME_REPORT_TRIGGERS = ["объем", "объём", "volume report", "/volume"];
const DECEL_REPORT_TRIGGERS = ["замедление", "decel report", "/decel"];

// Окна для замера дисбаланса объёма купли/продажи перед входом.
const VOLUME_WINDOWS_MS = [90 * 1000, 120 * 1000];

// Бакеты дисбаланса: (buyVol-sellVol)/(buyVol+sellVol), от -1 до 1.
const IMBALANCE_BUCKETS: { label: string; min: number; max: number }[] = [
  { label: "Сильная продажа (<-50%)", min: -1, max: -0.5 },
  { label: "Продажа (-50%..-10%)", min: -0.5, max: -0.1 },
  { label: "Нейтрально (-10%..+10%)", min: -0.1, max: 0.1 },
  { label: "Покупка (+10%..+50%)", min: 0.1, max: 0.5 },
  { label: "Сильная покупка (>+50%)", min: 0.5, max: 1 },
];
function imbalanceBucketLabel(imbalance: number): string {
  for (const b of IMBALANCE_BUCKETS) {
    if (imbalance >= b.min && imbalance < b.max) return b.label;
  }
  return IMBALANCE_BUCKETS[IMBALANCE_BUCKETS.length - 1].label; // ровно 1.0 попадает в последний
}

// Бакеты "замедления тейпа": ratio = recentMove10s / priorMove10s.
// ratio < 1 значит движение замедлилось перед входом, > 1 — ускорилось.
const DECEL_BUCKETS: { label: string; min: number; max: number }[] = [
  { label: "Сильное ускорение (>2x)", min: 2, max: Infinity },
  { label: "Ускорение (1.2x..2x)", min: 1.2, max: 2 },
  { label: "Стабильно (0.8x..1.2x)", min: 0.8, max: 1.2 },
  { label: "Замедление (0.4x..0.8x)", min: 0.4, max: 0.8 },
  { label: "Сильное замедление (<0.4x)", min: -Infinity, max: 0.4 },
];
function decelBucketLabel(ratio: number): string {
  for (const b of DECEL_BUCKETS) {
    if (ratio >= b.min && ratio < b.max) return b.label;
  }
  return DECEL_BUCKETS[DECEL_BUCKETS.length - 1].label;
}

type ComboMode = "fixed" | "adaptive";

interface TickDecel {
  recentMove10s: number | null; // движение монеты за 10с ДО входа
  priorMove10s: number | null; // движение монеты за 10с ДО ЭТОГО (т.е. [-20с, -10с])
}

interface ComboTradeEvent {
  mode: ComboMode;
  // fixed: param — это pctThreshold (доля, напр. 0.0013). adaptive: param — это multiplier (напр. 0.7).
  param: number;
  windowSec: number;
  eventSlug: string;
  coin: string;
  side: "Up" | "Down";
  entryTimestamp: number;
  determined: boolean;
  won: boolean | null;
  // Дисбаланс объёма купли/продажи на токене за N мс до входа (null,
  // если tradeFlowTracker ещё не успел накопить данные по этому токену).
  volumeImbalance: Record<number, number | null>; // ключ: окно в мс (из VOLUME_WINDOWS_MS)
  // recentVol монеты в момент входа — для fixed это просто справочная
  // информация (можно ли было бы отличить от adaptive-порога), для
  // adaptive — это и есть значение, из которого был выведен порог.
  volAtEntry: number | null;
  // Пассивный лог замедления тейпа — не влияет на критерии входа.
  tickDecel: TickDecel;
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

  // все комбо-сделки, ключ: `${mode}_${param}_${windowSec}:${eventSlug}:${side}`
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

      // Бэкфилл для записей из СТАРОЙ версии (до mode/adaptive/tickDecel):
      // раньше был только pctThreshold — превращаем в mode="fixed", param=pctThreshold.
      for (const t of this.tradesList as any[]) {
        if (t.mode === undefined) {
          t.mode = "fixed";
          t.param = t.pctThreshold ?? 0;
        }
        if (!t.volumeImbalance) t.volumeImbalance = {};
        if (t.volAtEntry === undefined) t.volAtEntry = null;
        if (!t.tickDecel) t.tickDecel = { recentMove10s: null, priorMove10s: null };
      }

      this.trades = new Map();
      for (const t of this.tradesList as ComboTradeEvent[]) {
        const key = `${t.mode}_${t.param}_${t.windowSec}:${t.eventSlug}:${t.side}`;
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
      `[refresh] наблюдаем (${INCLUDED_COINS.join("/")}) 5-мин рынков: ${markets.length} (${tokenIds.length} токенов), ` +
        `комбо-сделок открыто: ${this.tradesList.length}, ждём резолва: ${this.pendingResolution.size}`,
    );

    // tradeFlowTracker сам разберётся, что добавить/убрать из подписки —
    // вызываем всегда, независимо от того, поменялся ли набор токенов
    // для основного PriceWatcher.
    tradeFlowTracker.updateTokenIds(tokenIds);

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

  private recordEntryIfNew(
    mode: ComboMode,
    param: number,
    windowSec: number,
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    now: number,
    volAtEntry: number | null,
    tickDecel: TickDecel,
    volumeImbalance: Record<number, number | null>,
  ): void {
    const key = `${mode}_${param}_${windowSec}:${market.eventSlug}:${side}`;
    if (this.trades.has(key)) return; // уже зафиксирован вход для этой комбинации

    const trade: ComboTradeEvent = {
      mode,
      param,
      windowSec,
      eventSlug: market.eventSlug,
      coin: market.coin,
      side,
      entryTimestamp: now,
      determined: false,
      won: null,
      volumeImbalance,
      volAtEntry,
      tickDecel,
    };
    this.trades.set(key, trade);
    this.tradesList.push(trade);
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

    // Общие для всех новых входов на этом апдейте величины — считаем один
    // раз, а не на каждую комбинацию (recentVol и tickDecel не зависят от
    // конкретного порога/окна, только от монеты и текущего момента).
    let recentVol: number | null | undefined; // undefined = ещё не считали в этом апдейте
    let tickDecel: TickDecel | undefined;
    const getRecentVolLazy = (): number | null => {
      if (recentVol === undefined) recentVol = volTracker.getRecentVolatility(market.coin, now);
      return recentVol;
    };
    const getTickDecelLazy = (): TickDecel => {
      if (tickDecel === undefined) {
        const recentMove10s = volTracker.getMoveOverWindow(market.coin, now, TICK_DECEL_WINDOW_MS);
        const priorMove10s = volTracker.getMoveOverWindow(market.coin, now - TICK_DECEL_WINDOW_MS, TICK_DECEL_WINDOW_MS);
        tickDecel = { recentMove10s, priorMove10s };
      }
      return tickDecel;
    };
    let volumeImbalanceCache: Record<number, number | null> | undefined;
    const getVolumeImbalanceLazy = (): Record<number, number | null> => {
      if (volumeImbalanceCache === undefined) {
        const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
        volumeImbalanceCache = {};
        for (const windowMs of VOLUME_WINDOWS_MS) {
          const vi = tradeFlowTracker.getVolumeImbalance(tokenId, now, windowMs);
          volumeImbalanceCache[windowMs] = vi ? vi.imbalance : null;
        }
      }
      return volumeImbalanceCache;
    };

    for (const windowSec of WINDOWS_SEC) {
      if (secToClose > windowSec) continue; // ещё не дошли до окна входа этой комбинации

      // ── Сетка FIXED (как раньше) ──
      for (const pctThreshold of THRESHOLDS) {
        const passesMove = side === "Up" ? pctMove >= pctThreshold : pctMove <= -pctThreshold;
        if (!passesMove) continue;
        this.recordEntryIfNew(
          "fixed",
          pctThreshold,
          windowSec,
          market,
          side,
          now,
          getRecentVolLazy(),
          getTickDecelLazy(),
          getVolumeImbalanceLazy(),
        );
      }

      // ── Сетка ADAPTIVE (новое): порог = multiplier × recentVol ──
      const vol = getRecentVolLazy();
      if (vol !== null) {
        for (const multiplier of VOL_MULTIPLIERS) {
          const adaptiveThreshold = multiplier * vol;
          const passesMove = side === "Up" ? pctMove >= adaptiveThreshold : pctMove <= -adaptiveThreshold;
          if (!passesMove) continue;
          this.recordEntryIfNew(
            "adaptive",
            multiplier,
            windowSec,
            market,
            side,
            now,
            vol,
            getTickDecelLazy(),
            getVolumeImbalanceLazy(),
          );
        }
      }
      // vol === null значит recentVol ещё не набрал час истории для этой
      // монеты — adaptive-сетка для неё просто молчит, пока не накопится
      // (fixed-сетка при этом продолжает работать как раньше).
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

  /** Сводка по одной монете и одному режиму (fixed/adaptive): win/total на каждую комбинацию (только резолвнутые). */
  private gridForCoin(coin: string, mode: ComboMode): Map<string, { win: number; total: number }> {
    const params = mode === "fixed" ? THRESHOLDS : VOL_MULTIPLIERS;
    const grid = new Map<string, { win: number; total: number }>();
    for (const param of params) {
      for (const windowSec of WINDOWS_SEC) {
        grid.set(`${param}_${windowSec}`, { win: 0, total: 0 });
      }
    }
    for (const t of this.tradesList) {
      if (t.coin !== coin || !t.determined || t.mode !== mode) continue;
      const key = `${t.param}_${t.windowSec}`;
      const s = grid.get(key);
      if (!s) continue;
      s.total++;
      if (t.won) s.win++;
    }
    return grid;
  }

  /** Компактный отчёт: топ-5 по винрейту + лучшая по частоте при высоком винрейте, для каждой монеты. Общий для fixed/adaptive. */
  private buildCompactReportForMode(mode: ComboMode): string {
    const label = mode === "fixed" ? "FIXED (фикс. % порог)" : "ADAPTIVE (multiplier × recentVol)";
    const paramLabelFn = mode === "fixed" ? thresholdLabel : multiplierLabel;

    const lines: string[] = [];
    lines.push(`<b>📊 Отчёт [${label}]: сетка порог×окно (${INCLUDED_COINS.join("/")}, 5м, коридор ${PRICE_LOW}-${PRICE_HIGH}, держим до резолва)</b>`);
    lines.push(`Уникальных рынков обработано: ${this.marketsSeen.size} | Комбо-сделок всего (${mode}): ${this.tradesList.filter((t) => t.mode === mode).length}`);
    lines.push("");

    for (const coin of INCLUDED_COINS) {
      const grid = this.gridForCoin(coin, mode);

      const entries = [...grid.entries()]
        .map(([key, s]) => {
          const [paramStr, winStr] = key.split("_");
          return {
            param: Number(paramStr),
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
            `  ${i + 1}. ${paramLabelFn(e.param)} / ${e.windowSec}с — ${(e.winRate * 100).toFixed(0)}% (${e.win}/${e.total})`,
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
        lines.push(`  ${paramLabelFn(best.param)} / ${best.windowSec}с — ${best.total} сделок, ${(best.winRate * 100).toFixed(0)}%`);
      }
      lines.push("");
    }

    return lines.join("\n");
  }

  buildCompactReport(): string {
    return this.buildCompactReportForMode("fixed") + `\n\nПолная таблица: "${FULL_GRID_TRIGGERS[0]}" | Adaptive-отчёт: "${ADAPTIVE_REPORT_TRIGGERS[0]}"`;
  }

  buildAdaptiveCompactReport(): string {
    return this.buildCompactReportForMode("adaptive") + `\n\nПолная adaptive-таблица: "${ADAPTIVE_FULL_GRID_TRIGGERS[0]}"`;
  }

  /** Полная таблица — все комбинации, по монетам отдельно. Общая для fixed/adaptive. */
  private buildFullGridReportForMode(mode: ComboMode): string {
    const label = mode === "fixed" ? "FIXED" : "ADAPTIVE (× recentVol)";
    const params = mode === "fixed" ? THRESHOLDS : VOL_MULTIPLIERS;
    const paramLabelFn = mode === "fixed" ? thresholdLabel : multiplierLabel;

    const lines: string[] = [];
    lines.push(`<b>📊 Полная таблица [${label}]: сетка порог×окно (${INCLUDED_COINS.join("/")}, 5м, коридор ${PRICE_LOW}-${PRICE_HIGH})</b>`);
    lines.push("");

    for (const coin of INCLUDED_COINS) {
      const grid = this.gridForCoin(coin, mode);
      lines.push(`<b>═══ ${coin} ═══</b>`);
      lines.push("");

      const header = "Порог\\Окно  " + WINDOWS_SEC.map((w) => `${w}с`.padEnd(11)).join("");
      lines.push(`<pre>${header}</pre>`);

      for (const param of params) {
        const cells = WINDOWS_SEC.map((windowSec) => {
          const s = grid.get(`${param}_${windowSec}`)!;
          const cell = s.total > 0 ? `${s.win}/${s.total} ${(100 * s.win / s.total).toFixed(0)}%` : "—";
          return cell.padEnd(11);
        }).join("");
        lines.push(`<pre>${paramLabelFn(param).padEnd(11)}${cells}</pre>`);
      }
      lines.push("");
    }

    return lines.join("\n");
  }

  buildFullGridReport(): string {
    return this.buildFullGridReportForMode("fixed");
  }

  buildAdaptiveFullGridReport(): string {
    return this.buildFullGridReportForMode("adaptive");
  }

  /** Отчёт по дисбалансу объёма купли/продажи перед входом — по монетам и окнам замера (обе сетки вместе, дедуп по рынку). */
  buildVolumeReport(): string {
    const lines: string[] = [];
    lines.push(`<b>📊 Отчёт: объём покупок/продаж перед входом (окна ${VOLUME_WINDOWS_MS.map((ms) => `${ms / 1000}с`).join(" и ")})</b>`);
    lines.push("");

    for (const coin of INCLUDED_COINS) {
      for (const windowMs of VOLUME_WINDOWS_MS) {
        const windowSec = windowMs / 1000;
        lines.push(`<b>── ${coin}: винрейт по дисбалансу объёма — окно ${windowSec}с ──</b>`);

        // Берём каждую (eventSlug, side) только ОДИН раз — иначе одна и
        // та же рыночная ситуация посчитается многократно (по разу на
        // каждую комбинацию режим/порог/окно, у которых разное entryTimestamp).
        // Берём САМЫЙ РАННИЙ определившийся вход.
        const seen = new Map<string, ComboTradeEvent>();
        for (const t of this.tradesList) {
          if (t.coin !== coin || !t.determined) continue;
          const vi = t.volumeImbalance[windowMs];
          if (vi === null || vi === undefined) continue;
          const dedupeKey = `${t.eventSlug}:${t.side}`;
          const existing = seen.get(dedupeKey);
          if (!existing || t.entryTimestamp < existing.entryTimestamp) {
            seen.set(dedupeKey, t);
          }
        }

        const buckets = new Map<string, { win: number; total: number }>();
        for (const b of IMBALANCE_BUCKETS) buckets.set(b.label, { win: 0, total: 0 });

        for (const t of seen.values()) {
          const vi = t.volumeImbalance[windowMs] as number;
          const label = imbalanceBucketLabel(vi);
          const s = buckets.get(label)!;
          s.total++;
          if (t.won) s.win++;
        }

        let anyData = false;
        for (const b of IMBALANCE_BUCKETS) {
          const s = buckets.get(b.label)!;
          if (s.total === 0) continue;
          anyData = true;
          const pct = (100 * s.win) / s.total;
          lines.push(`  ${b.label.padEnd(28)} ${String(s.win).padStart(4)}/${String(s.total).padEnd(5)} ${pct.toFixed(0)}%`);
        }
        if (!anyData) lines.push("  пока недостаточно данных");
        lines.push("");
      }
    }

    return lines.join("\n");
  }

  /**
   * Отчёт по "замедлению тейпа" (пассивный лог, ничего не решал при входе):
   * ratio = recentMove10s / priorMove10s. <1 — движение замедлилось перед
   * входом, >1 — ускорилось. Дедуп по рынку, как в объёмном отчёте.
   */
  buildDecelReport(): string {
    const lines: string[] = [];
    lines.push(`<b>📊 Отчёт: замедление/ускорение движения монеты перед входом (окно ${TICK_DECEL_WINDOW_MS / 1000}с, пассивный лог)</b>`);
    lines.push("");

    for (const coin of INCLUDED_COINS) {
      lines.push(`<b>── ${coin}: винрейт по соотношению [движение за 10с до входа] / [движение за 10с до этого] ──</b>`);

      const seen = new Map<string, ComboTradeEvent>();
      for (const t of this.tradesList) {
        if (t.coin !== coin || !t.determined) continue;
        const { recentMove10s, priorMove10s } = t.tickDecel;
        if (recentMove10s === null || priorMove10s === null || priorMove10s === 0) continue;
        const dedupeKey = `${t.eventSlug}:${t.side}`;
        const existing = seen.get(dedupeKey);
        if (!existing || t.entryTimestamp < existing.entryTimestamp) {
          seen.set(dedupeKey, t);
        }
      }

      const buckets = new Map<string, { win: number; total: number }>();
      for (const b of DECEL_BUCKETS) buckets.set(b.label, { win: 0, total: 0 });

      for (const t of seen.values()) {
        const ratio = (t.tickDecel.recentMove10s as number) / (t.tickDecel.priorMove10s as number);
        const label = decelBucketLabel(ratio);
        const s = buckets.get(label)!;
        s.total++;
        if (t.won) s.win++;
      }

      let anyData = false;
      for (const b of DECEL_BUCKETS) {
        const s = buckets.get(b.label)!;
        if (s.total === 0) continue;
        anyData = true;
        const pct = (100 * s.win) / s.total;
        lines.push(`  ${b.label.padEnd(28)} ${String(s.win).padStart(4)}/${String(s.total).padEnd(5)} ${pct.toFixed(0)}%`);
      }
      if (!anyData) lines.push("  пока недостаточно данных");
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
      const fixedCount = this.tradesList.filter((t) => t.mode === "fixed").length;
      const adaptiveCount = this.tradesList.filter((t) => t.mode === "adaptive").length;
      console.log(
        `--- статус: апдейтов ${this.updateCount}, комбо-сделок fixed=${fixedCount} adaptive=${adaptiveCount} ---`,
      );
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
          console.log(`[telegram] Запрос компактного отчёта (fixed): "${msg.text}"`);
          await sendReportToTelegram(telegram, "<b>📊 Отчёт по запросу</b>", research.buildCompactReport());
          continue;
        }

        if (FULL_GRID_TRIGGERS.some((p) => text.includes(p.toLowerCase()))) {
          console.log(`[telegram] Запрос полной таблицы (fixed): "${msg.text}"`);
          await sendReportToTelegram(telegram, "<b>📊 Полная таблица по запросу</b>", research.buildFullGridReport());
          continue;
        }

        if (ADAPTIVE_REPORT_TRIGGERS.some((p) => text.includes(p.toLowerCase()))) {
          console.log(`[telegram] Запрос компактного отчёта (adaptive): "${msg.text}"`);
          await sendReportToTelegram(telegram, "<b>📊 Adaptive-отчёт по запросу</b>", research.buildAdaptiveCompactReport());
          continue;
        }

        if (ADAPTIVE_FULL_GRID_TRIGGERS.some((p) => text.includes(p.toLowerCase()))) {
          console.log(`[telegram] Запрос полной таблицы (adaptive): "${msg.text}"`);
          await sendReportToTelegram(telegram, "<b>📊 Полная adaptive-таблица по запросу</b>", research.buildAdaptiveFullGridReport());
          continue;
        }

        if (VOLUME_REPORT_TRIGGERS.some((p) => text.includes(p.toLowerCase()))) {
          console.log(`[telegram] Запрос отчёта по объёму: "${msg.text}"`);
          await sendReportToTelegram(telegram, "<b>📊 Отчёт по объёму по запросу</b>", research.buildVolumeReport());
          continue;
        }

        if (DECEL_REPORT_TRIGGERS.some((p) => text.includes(p.toLowerCase()))) {
          console.log(`[telegram] Запрос отчёта по замедлению: "${msg.text}"`);
          await sendReportToTelegram(telegram, "<b>📊 Отчёт по замедлению тейпа по запросу</b>", research.buildDecelReport());
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
  console.log("Исследовательский логгер СЕТКА v2 запущен (fixed + adaptive пороги, пассивный лог замедления тейпа).");
  console.log("Fixed-пороги (%):", THRESHOLDS.map(thresholdLabel).join(", "));
  console.log("Adaptive-мультипликаторы (× recentVol):", VOL_MULTIPLIERS.map(multiplierLabel).join(", "));
  console.log("Окна входа (с):", WINDOWS_SEC.join(", "));
  console.log(
    `Комбинаций на монету: fixed=${THRESHOLDS.length * WINDOWS_SEC.length}, adaptive=${VOL_MULTIPLIERS.length * WINDOWS_SEC.length}, монет: ${INCLUDED_COINS.length}`,
  );
  console.log("Adaptive-сетка начнёт давать сделки только после ~1 часа сбора истории волатильности по каждой монете.");

  // запускаем все фиды цены (нужны для расчёта % движения)
  btcPriceFeed.start();
  ethPriceFeed.start();
  solPriceFeed.start();
  xrpPriceFeed.start();
  dogePriceFeed.start();
  tradeFlowTracker.start();
  volTracker.start();

  const logger = createLogger(false);
  const telegram = createTelegramNotifier(process.env.TELEGRAM_BOT_TOKEN, process.env.TELEGRAM_CHAT_ID, logger);

  const research = new ResearchGridLogger();
  research.start();

  if (telegram && process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID) {
    console.log(
      `Telegram включён — "${COMPACT_REPORT_TRIGGERS[0]}" (fixed кратко), "${FULL_GRID_TRIGGERS[0]}" (fixed таблица), ` +
        `"${ADAPTIVE_REPORT_TRIGGERS[0]}" (adaptive кратко), "${ADAPTIVE_FULL_GRID_TRIGGERS[0]}" (adaptive таблица), ` +
        `"${VOLUME_REPORT_TRIGGERS[0]}" (объём), "${DECEL_REPORT_TRIGGERS[0]}" (замедление тейпа).`,
    );
    pollTelegramCommands(process.env.TELEGRAM_BOT_TOKEN, process.env.TELEGRAM_CHAT_ID, telegram, research);

    console.log(`Автоотчёт (краткий, fixed) включён — каждые ${AUTO_REPORT_INTERVAL_MS / 60000} минут.`);
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
    console.log("\n" + research.buildAdaptiveCompactReport());
    research.saveState();
    if (telegram) {
      await sendReportToTelegram(telegram, "<b>🌙 Финальный отчёт за ночь (fixed)</b>", research.buildCompactReport());
      await sendReportToTelegram(telegram, "<b>🌙 Финальный отчёт за ночь (adaptive)</b>", research.buildAdaptiveCompactReport());
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
