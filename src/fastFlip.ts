/**
 * "Быстрый флип" v6.3 — РЫНОЧНЫЕ ордера на ВХОД. Сразу после входа
 * выставляется ЛИМИТНАЯ заявка на ВЫХОД по exitPrice (по умолчанию 0.99).
 * Пока эта лимитка не исполнилась и рынок не зарезолвился — позиция
 * считается открытой. Аварийных стопов и продаж по тику цены как не
 * было, так и нет.
 *
 * v6.3 (относительно v6.2): ДВЕ НОВЫЕ ВЕЩИ.
 *
 * 1) В начале каждого часа (и сразу при старте бота, если час уже идёт)
 *    случайно выбирается N пятиминуток из 12 возможных в этом часе,
 *    где N = квота сделок в час (quotaPerHour). Бот пытается входить
 *    ТОЛЬКО в выбранные пятиминутки. Если в выбранной пятиминутке
 *    условия входа так и не сложились (цена не дошла до коридора) —
 *    просто ждём следующую ВЫБРАННУЮ пятиминутку, остальные (не
 *    попавшие в выборку) полностью игнорируются.
 *
 * 2) Сразу как только рыночная покупка исполнилась, бот СРАЗУ выставляет
 *    лимитную заявку на продажу (GTC) по settings.exitPrice (по
 *    умолчанию 0.99) на весь объём позиции. Если выставить заявку не
 *    получилось (ошибка биржи/сети) — бот повторяет попытку каждые
 *    EXIT_ORDER_RETRY_MS, пока заявка не будет успешно выставлена.
 *    Это НЕ рыночная продажа и не реакция на тик цены — это пассивная
 *    лимитка, которая исполнится сама, только если рынок реально
 *    дойдёт до этой цены. Поэтому проблема v6.1 (стоп срабатывал от
 *    единичного шумового тика) сюда не относится.
 *
 *    ВАЖНО: bot.clob.placeLimitOrder() в этом файле изначально писался
 *    под агрессивный "рыночный" вход (с maxSlippagePct, ожидание
 *    немедленного/частичного исполнения). Для пассивной лимитки на
 *    выход этот же метод вызывается с maxSlippagePct: 0 — то есть
 *    ожидается, что он просто выставит GTC-ордер ровно по exitPrice.
 *    Если в реальности ClobService.placeLimitOrder ведёт себя иначе
 *    (например, всегда пытается исполниться немедленно и отменяет
 *    остаток вместо того чтобы оставить его в стакане) — нужно
 *    смотреть в clob.ts и либо добавить отдельный метод
 *    placeRestingLimitOrder(...), либо передавать туда явный флаг
 *    "GTC" / orderType. Я не видел содержимое clob.ts, поэтому это
 *    единственное место, которое стоит перепроверить руками.
 *
 *    Также: если лимитка на выход исполнится ДО официального резолва
 *    рынка, эта версия бота узнает об этом? — НЕТ. Бот не опрашивает
 *    статус выставленной заявки и не проверяет, продалась ли позиция
 *    раньше времени. Он всё равно дождётся закрытия окна и запросит
 *    resolveWinner(), и посчитает профит так, как будто держал до
 *    резолва (по buyPrice), даже если на самом деле уже продал по
 *    exitPrice раньше. Отчёт в Telegram в этом случае будет неточным
 *    (хотя реальные деньги на балансе будут в порядке — это только
 *    вопрос корректности итогового сообщения/статистики). Если нужно,
 *    это лечится отдельно — опросом DataApiClient.getPositions() на
 *    предмет "позиция уже закрыта" перед вызовом resolveWinner().
 */

import "dotenv/config";
import { Side } from "@polymarket/clob-client-v2";
import { discoverCryptoUpDownMarkets, CryptoUpDownMarket } from "./cryptoMarketDiscovery.js";
import { PriceWatcher, PriceUpdate } from "./priceWatcher.js";
import { ClobService } from "./clob.js";
import { RedeemService } from "./redeem.js";
import { DataApiClient } from "./dataApi.js";
import { createTelegramNotifier } from "./telegram.js";
import { createLogger } from "./logger.js";

const DRY_RUN = (process.env.FASTFLIP_DRY_RUN ?? "true").toLowerCase() !== "false";
const AUTO_REDEEM = (process.env.FASTFLIP_AUTO_REDEEM ?? "true").toLowerCase() !== "false";
const TRADE_SIZE_USD = Number(process.env.FASTFLIP_TRADE_SIZE_USD ?? "5");

const COIN = "Bitcoin";
const TARGET_WINDOW_MINUTES = 5;
const WINDOWS_PER_HOUR = 60 / TARGET_WINDOW_MINUTES; // 12 пятиминуток в часе

const TIMEFRAMES_TO_DISCOVER = [{ suffixes: ["up-or-down-5m"], minutes: TARGET_WINDOW_MINUTES }];

const HOUR_MS = 60 * 60 * 1000;

const MARKET_REFRESH_MS = 15 * 1000;
const REFRESH_SAFETY_BUFFER_SEC = 30;

function observeWindowMs(windowMinutes: number): number {
  return (windowMinutes + 1) * 60 * 1000;
}

const MARKET_ORDER_SLIPPAGE_PCT = Number(process.env.FASTFLIP_SLIPPAGE_PCT ?? "0.5");

const CLOSE_FALLBACK_BUFFER_MS = 5 * 1000;

const RESOLVE_CHECK_DELAY_SEC = 180;
const RESOLVE_RETRY_MS = 30 * 1000;
const RESOLVE_GIVE_UP_MS = 30 * 60 * 1000;

const EXIT_ORDER_RETRY_MS = 5 * 1000;

const REDEEM_POLL_MS = 60 * 1000;
const GAMMA_HOST = "https://gamma-api.polymarket.com";

// ─── Настройки, которые можно менять на лету через Telegram ───
const settings = {
  entryPrice: Number(process.env.FASTFLIP_ENTRY_PRICE ?? "0.97"),
  maxEntryPrice: Number(process.env.FASTFLIP_MAX_ENTRY_PRICE ?? "0.98"),
  entryWindowSec: Number(process.env.FASTFLIP_ENTRY_WINDOW_SEC ?? "60"),
  quotaPerHour: Math.max(1, Number(process.env.FASTFLIP_QUOTA_PER_HOUR ?? "999")),
  exitPrice: Number(process.env.FASTFLIP_EXIT_PRICE ?? "0.99"),
};

interface TokenInfo {
  market: CryptoUpDownMarket;
  side: "Up" | "Down";
}

interface OpenPosition {
  market: CryptoUpDownMarket;
  side: "Up" | "Down";
  tokenId: string;
  buyPrice: number;
  filledSize: number;
  closed: boolean;
  minPriceSinceEntry: number;
  resolveFallbackTimer: NodeJS.Timeout | null;
  exitOrderPlaced: boolean;
}

function buildTokenIndex(markets: CryptoUpDownMarket[]): Map<string, TokenInfo> {
  const idx = new Map<string, TokenInfo>();
  for (const m of markets) {
    idx.set(m.upTokenId, { market: m, side: "Up" });
    idx.set(m.downTokenId, { market: m, side: "Down" });
  }
  return idx;
}

/** На какую пятиминутку внутри часа приходится ОТКРЫТИЕ этого рынка (0..11). */
function windowIndexInHour(market: CryptoUpDownMarket): number {
  const startMs = market.closeTimeMs - market.windowMinutes * 60 * 1000;
  const hourStartMs = Math.floor(startMs / HOUR_MS) * HOUR_MS;
  return Math.floor((startMs - hourStartMs) / (TARGET_WINDOW_MINUTES * 60 * 1000));
}

/** Официальный резолв рынка через Gamma API. Это ЕДИНСТВЕННЫЙ способ закрыть сделку. */
async function resolveWinner(eventSlug: string): Promise<"Up" | "Down" | null> {
  try {
    const resp = await fetch(`${GAMMA_HOST}/events/slug/${eventSlug}`);
    if (!resp.ok) return null;
    const event = await resp.json();
    const market = (event.markets ?? [])[0];
    if (!market) return null;

    let outcomes: string[];
    let outcomePrices: string[];
    try {
      outcomes = JSON.parse(market.outcomes ?? "[]");
      outcomePrices = JSON.parse(market.outcomePrices ?? "[]");
    } catch {
      return null;
    }
    if (outcomes.length !== 2 || outcomePrices.length !== 2) return null;

    const upIdx = outcomes.findIndex((o) => /^up$/i.test(o.trim()));
    const downIdx = outcomes.findIndex((o) => /^down$/i.test(o.trim()));
    if (upIdx === -1 || downIdx === -1) return null;

    const upPrice = Number(outcomePrices[upIdx]);
    const downPrice = Number(outcomePrices[downIdx]);
    if (upPrice > 0.05 && upPrice < 0.95) return null; // ещё не устаканилось

    return upPrice > downPrice ? "Up" : "Down";
  } catch (err) {
    console.error(`[resolve] ошибка ${eventSlug}:`, (err as Error).message);
    return null;
  }
}

interface MarketOrderResult {
  orderId: string | null;
  filledSize: number;
  avgPrice: number;
}

function resolveFill(
  rawA: number,
  rawB: number,
  expectedPrice: number,
): { filledSize: number; avgPrice: number } {
  const candidates: { shares: number; dollars: number; price: number }[] = [];

  for (const [shares, dollars] of [
    [rawA, rawB],
    [rawB, rawA],
  ] as const) {
    if (shares <= 0) continue;
    const price = dollars / shares;
    if (price > 0 && price < 1) {
      candidates.push({ shares, dollars, price });
    }
  }

  if (candidates.length === 0) {
    return { filledSize: 0, avgPrice: 0 };
  }

  candidates.sort((a, b) => Math.abs(a.price - expectedPrice) - Math.abs(b.price - expectedPrice));

  const best = candidates[0];
  return { filledSize: best.shares, avgPrice: best.price };
}

class FastFlipMarketBot {
  private watcher: PriceWatcher | null = null;
  private tokenIndex = new Map<string, TokenInfo>();
  private lastTokenIds: string[] = [];

  private attemptInProgress = false;

  private openPosition: OpenPosition | null = null;
  private updateCount = 0;
  private tradesTotal = 0;

  private currentHourKey: number | null = null;
  private tradesThisHour = 0;

  // v6.3: набор выбранных на этот час пятиминуток (индексы 0..11)
  private windowsHourKey: number | null = null;
  private selectedWindows: Set<number> = new Set();

  constructor(
    private clob: ClobService | null,
    private telegram: ReturnType<typeof createTelegramNotifier>,
  ) {}

  getStatus(): string {
    const watchedMarkets = this.tokenIndex.size / 2;
    const windowsLabel = [...this.selectedWindows]
      .sort((a, b) => a - b)
      .map((i) => `${String(i * TARGET_WINDOW_MINUTES).padStart(2, "0")}`)
      .join(", ");
    return (
      `Режим: рыночный вход, лимитка на выход по ${settings.exitPrice}, иначе держим до резолва, BTC\n` +
      `Вход: ${settings.entryPrice}–${settings.maxEntryPrice} | Окно входа: последние ${settings.entryWindowSec}с | Выход: лимитка ${settings.exitPrice}\n` +
      `Квота: ${this.tradesThisHour}/${settings.quotaPerHour} сделок в этом часе\n` +
      `Выбранные пятиминутки этого часа (мин. от начала часа): ${windowsLabel || "—"}\n` +
      `Сейчас отслеживается активных 5-мин рынков: ${watchedMarkets}\n` +
      `Всего сделок с запуска: ${this.tradesTotal}\n` +
      `Открытая позиция: ${
        this.openPosition
          ? `${this.openPosition.market.title} (${this.openPosition.side}), цена входа ${this.openPosition.buyPrice.toFixed(4)}, ` +
            `выход: ${this.openPosition.exitOrderPlaced ? `лимитка по ${settings.exitPrice} стоит` : "выставляю лимитку..."}`
          : "нет"
      }`
    );
  }

  private checkHourlyReset(): void {
    const hourKey = Math.floor(Date.now() / HOUR_MS);
    if (this.currentHourKey !== hourKey) {
      this.currentHourKey = hourKey;
      this.tradesThisHour = 0;
    }
    this.regenerateSelectedWindowsIfNeeded(hourKey);
  }

  /** v6.3: раз в час случайно выбираем quotaPerHour пятиминуток из 12, в которые будем пытаться входить. */
  private regenerateSelectedWindowsIfNeeded(hourKey: number): void {
    if (this.windowsHourKey === hourKey) return;
    this.windowsHourKey = hourKey;

    const all = Array.from({ length: WINDOWS_PER_HOUR }, (_, i) => i);
    for (let i = all.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [all[i], all[j]] = [all[j], all[i]];
    }
    const count = Math.min(Math.max(1, settings.quotaPerHour), WINDOWS_PER_HOUR);
    this.selectedWindows = new Set(all.slice(0, count));

    const label = [...this.selectedWindows]
      .sort((a, b) => a - b)
      .map((i) => `${String(i * TARGET_WINDOW_MINUTES).padStart(2, "0")}`)
      .join(", ");
    console.log(`[окна часа] Новый час — выбраны пятиминутки для торговли (мин.): ${label}`);
    if (this.telegram) {
      this.telegram.send(`🎲 Новый час: буду пытаться входить только в пятиминутки: ${label}`).catch(() => {});
    }
  }

  private hasCriticalMarket(): boolean {
    const now = Date.now();
    const criticalMs = (settings.entryWindowSec + REFRESH_SAFETY_BUFFER_SEC) * 1000;
    for (const info of this.tokenIndex.values()) {
      const msToClose = info.market.closeTimeMs - now;
      if (msToClose >= 0 && msToClose <= criticalMs) return true;
    }
    return false;
  }

  async refreshMarkets(): Promise<void> {
    if (this.openPosition || this.attemptInProgress || this.hasCriticalMarket()) return;

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
        m.coin.toUpperCase() === COIN.toUpperCase() &&
        m.windowMinutes === TARGET_WINDOW_MINUTES &&
        m.closeTimeMs - now <= observeWindowMs(m.windowMinutes),
    );

    this.tokenIndex = buildTokenIndex(markets);
    const tokenIds = [...this.tokenIndex.keys()].sort();

    console.log(`[refresh] наблюдаем активных BTC 5-мин рынков: ${markets.length} (${tokenIds.length} токенов)`);

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

    // Позиция уже открыта — никакой реакции на цену вообще, кроме
    // отслеживания минимума (для статистики просадки в отчёте о
    // резолве). Продажа по цене происходит ТОЛЬКО через отдельно
    // выставленную лимитку на выход (см. placeExitLimitOrder), а не
    // отсюда.
    if (this.openPosition) {
      if (market.eventSlug === this.openPosition.market.eventSlug && price < this.openPosition.minPriceSinceEntry) {
        this.openPosition.minPriceSinceEntry = price;
      }
      return;
    }

    if (this.attemptInProgress) return;

    this.checkHourlyReset();

    if (this.tradesThisHour >= settings.quotaPerHour) return; // квота часа выполнена

    // v6.3: торгуем только в заранее выбранные на этот час пятиминутки
    if (!this.selectedWindows.has(windowIndexInHour(market))) return;

    const secToClose = (market.closeTimeMs - Date.now()) / 1000;

    if (secToClose > settings.entryWindowSec || secToClose < 0) return;

    if (price < settings.entryPrice) return;
    if (price > settings.maxEntryPrice) return;

    this.attemptInProgress = true;
    const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
    this.executeMarketEntry(market, side, tokenId, price, secToClose);
  }

  private async placeMarketOrder(params: {
    tokenId: string;
    side: "BUY" | "SELL";
    size: number;
    nominalPrice: number;
  }): Promise<MarketOrderResult> {
    if (DRY_RUN || !this.clob) {
      return { orderId: null, filledSize: params.size, avgPrice: params.nominalPrice };
    }

    try {
      const resp = await this.clob.placeLimitOrder({
        tokenId: params.tokenId,
        side: params.side === "BUY" ? Side.BUY : Side.SELL,
        price: params.nominalPrice,
        size: params.size,
        maxSlippagePct: MARKET_ORDER_SLIPPAGE_PCT,
      });

      const rawA = Number(resp.filledSize ?? 0);
      const rawB = Number(resp.filledUsdc ?? 0);

      const { filledSize, avgPrice } = resolveFill(rawA, rawB, params.nominalPrice);

      if (filledSize > 0) {
        console.log(
          `   [fill] сырые числа биржи: ${rawA} / ${rawB} → разобрано как ${filledSize.toFixed(4)} акций по ${avgPrice.toFixed(4)} (ожидали ~${params.nominalPrice.toFixed(4)})`,
        );
      }

      return { orderId: null, filledSize, avgPrice };
    } catch (err) {
      console.log(`   ⏳ Рыночный ордер (${params.side}) не исполнился: ${(err as Error).message}`);
      return { orderId: null, filledSize: 0, avgPrice: 0 };
    }
  }

  /**
   * v6.3: сразу после входа выставляем ПАССИВНУЮ лимитку на продажу
   * (GTC) по settings.exitPrice на весь объём позиции. Если не
   * получилось выставить — повторяем, пока не получится (или пока
   * позиция не закрылась резолвом раньше, чем заявка успела встать).
   */
  private async placeExitLimitOrder(pos: OpenPosition): Promise<void> {
    if (DRY_RUN || !this.clob) {
      pos.exitOrderPlaced = true;
      console.log(`   🎯 [DRY_RUN] Лимитка на выход по ${settings.exitPrice} выставлена (симуляция).`);
      return;
    }

    while (!pos.closed) {
      try {
        await this.clob.placeLimitOrder({
          tokenId: pos.tokenId,
          side: Side.SELL,
          price: settings.exitPrice,
          size: pos.filledSize,
          // 0 = не гнаться за немедленным исполнением, это пассивная
          // GTC-заявка ровно по exitPrice, ждущая реальной цены.
          maxSlippagePct: 0,
        });
        pos.exitOrderPlaced = true;
        console.log(
          `   🎯 Лимитка на выход выставлена: ПРОДАЖА ${pos.filledSize.toFixed(2)} акций по ${settings.exitPrice} (eventSlug: ${pos.market.eventSlug}).`,
        );
        if (this.telegram) {
          await this.telegram.send(
            `🎯 Выставлена лимитка на выход по ${settings.exitPrice}\n${pos.market.title}\nСторона: ${pos.side}\nРазмер: ${pos.filledSize.toFixed(2)}`,
          );
        }
        return;
      } catch (err) {
        console.log(
          `   ⏳ Не удалось выставить лимитку на выход (${(err as Error).message}). Повтор через ${EXIT_ORDER_RETRY_MS / 1000}с...`,
        );
        await new Promise((r) => setTimeout(r, EXIT_ORDER_RETRY_MS));
      }
    }
  }

  private async executeMarketEntry(
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    tokenId: string,
    priceAtEntry: number,
    secToClose: number,
  ): Promise<void> {
    const size = TRADE_SIZE_USD / settings.entryPrice;

    console.log(
      `\n⚡ ВХОД ПО РЫНКУ: [BTC / 5мин] "${market.title}"\n` +
        `   Сторона: ${side} | Цена сейчас: ~${priceAtEntry} (коридор ${settings.entryPrice}-${settings.maxEntryPrice}) | До закрытия: ${secToClose.toFixed(1)}с\n` +
        `   Покупаем: ${size.toFixed(2)} акций рыночным ордером (~$${TRADE_SIZE_USD})`,
    );

    const result = await this.placeMarketOrder({ tokenId, side: "BUY", size, nominalPrice: priceAtEntry });

    if (result.filledSize <= 0) {
      console.log(`   ⏳ Рыночная покупка не исполнилась (eventSlug: ${market.eventSlug}). Продолжаю мониторинг.`);
      this.attemptInProgress = false;
      return;
    }

    console.log(`   💰 ПОКУПКА ИСПОЛНЕНА ПО РЫНКУ: ${result.filledSize.toFixed(2)} акций по ~${result.avgPrice.toFixed(4)}.`);

    const pos: OpenPosition = {
      market,
      side,
      tokenId,
      buyPrice: result.avgPrice,
      filledSize: result.filledSize,
      closed: false,
      minPriceSinceEntry: result.avgPrice,
      resolveFallbackTimer: null,
      exitOrderPlaced: false,
    };
    this.openPosition = pos;

    if (this.telegram) {
      await this.telegram.send(
        `💰 Куплено по рынку: ${market.title}\nСторона: ${side}\nЦена: ${result.avgPrice.toFixed(4)} | Размер: ${result.filledSize.toFixed(2)}\nСразу выставляю лимитку на выход по ${settings.exitPrice}. Если не сработает — держим до официального резолва.`,
      );
    }

    // v6.3: выставляем лимитку на выход СРАЗУ, не дожидаясь ничего
    // другого. Не await — чтобы не блокировать таймер фолбэка ниже.
    this.placeExitLimitOrder(pos).catch((err) =>
      console.error("[exit] неожиданная ошибка при выставлении лимитки:", (err as Error).message),
    );

    const msUntilCloseCheck = Math.max(0, market.closeTimeMs - Date.now() + CLOSE_FALLBACK_BUFFER_MS);
    pos.resolveFallbackTimer = setTimeout(() => {
      if (pos.closed) return;
      console.log(`   ⏳ Окно закрылось — жду официальный резолв (eventSlug: ${market.eventSlug}).`);
      this.scheduleResolveFallback(pos);
    }, msUntilCloseCheck);
  }

  private scheduleResolveFallback(pos: OpenPosition): void {
    const startedAt = Date.now();
    const check = async () => {
      if (pos.closed) return;
      const now = Date.now();
      if (now - pos.market.closeTimeMs < RESOLVE_CHECK_DELAY_SEC * 1000) {
        setTimeout(check, RESOLVE_RETRY_MS);
        return;
      }
      const winner = await resolveWinner(pos.market.eventSlug);
      if (winner === null) {
        if (now - startedAt >= RESOLVE_GIVE_UP_MS) {
          console.log(`   ⚠️ Резолв так и не пришёл за отведённое время (eventSlug: ${pos.market.eventSlug}).`);
          if (this.telegram) {
            await this.telegram.send(
              `⚠️ Не удалось узнать исход сделки: ${pos.market.title}\nСторона: ${pos.side}\nПроверь вручную.`,
            );
          }
          pos.closed = true;
          this.finishTrade(pos);
          return;
        }
        setTimeout(check, RESOLVE_RETRY_MS);
        return;
      }
      pos.closed = true;
      const outcome: "WIN" | "LOSS" = winner === pos.side ? "WIN" : "LOSS";
      const profit = outcome === "WIN" ? pos.filledSize * (1 - pos.buyPrice) : -pos.filledSize * pos.buyPrice;
      await this.notifyClose(pos, "резолв рынка", outcome, profit);
      this.finishTrade(pos);
    };
    setTimeout(check, RESOLVE_RETRY_MS);
  }

  private finishTrade(pos: OpenPosition): void {
    if (pos.resolveFallbackTimer) {
      clearTimeout(pos.resolveFallbackTimer);
      pos.resolveFallbackTimer = null;
    }
    if (this.openPosition === pos) this.openPosition = null;
    this.attemptInProgress = false;
    this.tradesTotal++;

    this.checkHourlyReset();
    this.tradesThisHour++;

    console.log(
      `✅ Сделка завершена (всего с запуска: ${this.tradesTotal}, в этом часе: ${this.tradesThisHour}/${settings.quotaPerHour}). Возвращаюсь к мониторингу всех активных рынков.`,
    );

    this.refreshMarkets().catch((err) => console.error("[refresh] ошибка:", (err as Error).message));
  }

  private async notifyClose(
    pos: OpenPosition,
    how: string,
    outcome: "WIN" | "LOSS",
    profit: number,
  ): Promise<void> {
    const sign = outcome === "WIN" ? "✅ ПРИБЫЛЬ" : "🔻 УБЫТОК";

    const drawdown = Math.max(0, pos.buyPrice - pos.minPriceSinceEntry);
    const drawdownPct = pos.buyPrice > 0 ? (drawdown / pos.buyPrice) * 100 : 0;

    const msg =
      `${sign} (${outcome})\n` +
      `Способ закрытия: ${how}\n` +
      `${pos.market.title}\n` +
      `Сторона: ${pos.side}\n` +
      `Профит: ${profit >= 0 ? "+" : ""}$${profit.toFixed(3)}\n` +
      `Цена покупки: ${pos.buyPrice.toFixed(4)} | Мин. цена за сделку: ${pos.minPriceSinceEntry.toFixed(4)}\n` +
      `Максимальная просадка: ${drawdown.toFixed(3)} (${drawdownPct.toFixed(1)}%)`;

    console.log(`\n${msg}\n`);
    if (this.telegram) await this.telegram.send(msg);
  }

  start(): void {
    this.checkHourlyReset(); // сразу выбрать пятиминутки текущего часа при старте
    this.refreshMarkets();
    setInterval(() => this.refreshMarkets(), MARKET_REFRESH_MS);
    setInterval(() => {
      console.log(
        `--- статус: апдейтов ${this.updateCount}, отслеживается рынков: ${this.tokenIndex.size / 2}, ` +
          `сделок в часе: ${this.tradesThisHour}/${settings.quotaPerHour}, всего: ${this.tradesTotal} ---`,
      );
    }, 30 * 1000);
  }
}

async function redeemLoop(): Promise<void> {
  const rpcUrl = process.env.RPC_URL;
  const profileAddress = process.env.PROFILE_ADDRESS ?? process.env.FUNDER_ADDRESS;
  if (!rpcUrl || !profileAddress) {
    console.warn("[redeem] RPC_URL или PROFILE_ADDRESS не заданы — авто-клейм отключён.");
    return;
  }

  const logger = createLogger(false);
  const dataApi = new DataApiClient(process.env.DATA_API_HOST ?? "https://data-api.polymarket.com", logger);

  const apiKey = process.env.BUILDER_API_KEY;
  const apiSecret = process.env.BUILDER_API_SECRET;
  const apiPassphrase = process.env.BUILDER_API_PASSPHRASE;
  const builderCreds =
    apiKey && apiSecret && apiPassphrase ? { key: apiKey, secret: apiSecret, passphrase: apiPassphrase } : undefined;

  const redeemService = RedeemService.init(
    {
      relayerUrl: process.env.RELAYER_URL ?? "https://relayer-v2.polymarket.com",
      chainId: Number(process.env.CHAIN_ID ?? "137"),
      privateKey: process.env.PRIVATE_KEY!,
      rpcUrl,
      txType: (process.env.RELAYER_TX_TYPE as "SAFE" | "PROXY") ?? "PROXY",
      builderCreds,
    },
    logger,
  );

  console.log("[redeem] Авто-клейм выигрышей запущен.");
  const attempted = new Set<string>();

  for (;;) {
    try {
      const positions = await dataApi.getPositions(profileAddress, true);
      const eligible = positions.filter((p) => !attempted.has(p.conditionId));
      if (eligible.length > 0) {
        const txHashes = await redeemService.redeemPositions(eligible);
        for (const p of eligible) attempted.add(p.conditionId);
        if (txHashes.length > 0) console.log(`[redeem] ✅ Заклеймлено: ${txHashes.length}`, txHashes);
      }
    } catch (err) {
      console.error("[redeem] ошибка:", (err as Error).message);
    }
    await new Promise((r) => setTimeout(r, REDEEM_POLL_MS));
  }
}

/** Слушает команды в Telegram и меняет settings на лету. */
async function pollTelegramCommands(
  botToken: string,
  chatId: string,
  telegram: ReturnType<typeof createTelegramNotifier>,
  bot: FastFlipMarketBot,
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
        const text = msg.text.trim().toLowerCase();

        if (text === "статус") {
          await telegram?.send(bot.getStatus());
          continue;
        }

        const priceMatch = text.match(/^цена\s+([\d.]+)$/);
        if (priceMatch) {
          settings.entryPrice = Number(priceMatch[1]);
          await telegram?.send(`Нижняя цена входа установлена: ${settings.entryPrice}`);
          continue;
        }

        const maxPriceMatch = text.match(/^цена_макс\s+([\d.]+)$/);
        if (maxPriceMatch) {
          settings.maxEntryPrice = Number(maxPriceMatch[1]);
          await telegram?.send(`Верхняя цена входа установлена: ${settings.maxEntryPrice}`);
          continue;
        }

        const exitMatch = text.match(/^выход\s+([\d.]+)$/);
        if (exitMatch) {
          settings.exitPrice = Number(exitMatch[1]);
          await telegram?.send(`Цена выхода (лимитка) установлена: ${settings.exitPrice}`);
          continue;
        }

        const windowMatch = text.match(/^окно\s+(\d+)$/);
        if (windowMatch) {
          settings.entryWindowSec = Number(windowMatch[1]);
          await telegram?.send(`Окно входа установлено: последние ${settings.entryWindowSec}с перед закрытием`);
          continue;
        }

        const quotaMatch = text.match(/^квота\s+(\d+)$/);
        if (quotaMatch) {
          settings.quotaPerHour = Math.max(1, Number(quotaMatch[1]));
          await telegram?.send(
            `Квота установлена: ${settings.quotaPerHour} сделок в час. Новая выборка пятиминуток вступит в силу со следующего часа.`,
          );
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
  // МЕТКА ВЕРСИИ — если в логах при старте бота НЕТ этой строки,
  // значит запущен не этот файл (старая сборка / другой процесс).
  console.log("=== FASTFLIP BUILD: v6.3 — СЛУЧАЙНЫЕ ПЯТИМИНУТКИ В ЧАСЕ + МГНОВЕННАЯ ЛИМИТКА НА ВЫХОД ===");
  console.log(`Режим: ${DRY_RUN ? "DRY_RUN (без реальных сделок, полная симуляция)" : "⚠️  LIVE — РЕАЛЬНЫЕ ДЕНЬГИ"}`);
  console.log(
    `Актив: BTC only | Вход: ${settings.entryPrice}-${settings.maxEntryPrice} (рынком, окно ${settings.entryWindowSec}с) | ` +
      `Выход: лимитка по ${settings.exitPrice}, иначе официальный резолв. | Квота: ${settings.quotaPerHour}/час (${WINDOWS_PER_HOUR} пятиминуток на выбор)`,
  );

  let clob: ClobService | null = null;
  if (!DRY_RUN) {
    const logger = createLogger(false);
    const apiKey = process.env.CLOB_API_KEY;
    const apiSecret = process.env.CLOB_API_SECRET;
    const apiPassphrase = process.env.CLOB_API_PASSPHRASE;
    const apiCreds = apiKey && apiSecret && apiPassphrase ? { key: apiKey, secret: apiSecret, passphrase: apiPassphrase } : undefined;

    clob = await ClobService.init(
      {
        host: process.env.CLOB_HOST ?? "https://clob.polymarket.com",
        rpcUrl: process.env.RPC_URL,
        chainId: Number(process.env.CHAIN_ID ?? "137"),
        privateKey: process.env.PRIVATE_KEY!,
        signatureType: Number(process.env.SIGNATURE_TYPE ?? "1"),
        funderAddress: process.env.FUNDER_ADDRESS ?? process.env.PROFILE_ADDRESS,
        apiCreds,
      },
      logger,
    );
    console.log("ClobService инициализирован для LIVE торговли.");
  }

  const logger = createLogger(false);
  const telegram = createTelegramNotifier(process.env.TELEGRAM_BOT_TOKEN, process.env.TELEGRAM_CHAT_ID, logger);

  const bot = new FastFlipMarketBot(clob, telegram);
  bot.start();

  if (telegram && process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID) {
    console.log("Telegram-команды включены: цена X / цена_макс X / выход X / окно X / квота X / статус");
    pollTelegramCommands(process.env.TELEGRAM_BOT_TOKEN, process.env.TELEGRAM_CHAT_ID, telegram, bot);
  }

  if (!DRY_RUN && AUTO_REDEEM) {
    redeemLoop();
  }
}

main().catch((err) => {
  console.error("Фатальная ошибка:", err);
  process.exit(1);
});