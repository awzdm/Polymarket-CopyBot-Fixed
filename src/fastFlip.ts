/**
 * "Быстрый флип" v7 — торговля АЛЬТКОИНАМИ (всё, кроме BTC), вход 0.97,
 * выход 0.98 (или 0.99, настраивается), БЕЗ привязки к последним
 * секундам перед закрытием, С квотой сделок в час. ПАРАЛЛЕЛЬНЫЙ
 * sniperTrader.ts / fastFlip.ts (их не трогаем, не запускаем).
 *
 * ПОЧЕМУ ИМЕННО ТАК (по итогам анализа собранной статистики):
 *
 *  1. МОНЕТЫ: BTC исключён. Статистика по ETH/SOL/XRP/DOGE на связке
 *     0.97→0.98 показала самую низкую просадку и самый стабильный win
 *     rate (99.4%, почти без разброса по монете, часу суток и времени
 *     входа) из всех вариантов, что мы тестировали, включая BTC.
 *
 *  2. БЕЗ ОГРАНИЧЕНИЯ "ТОЛЬКО ПОСЛЕДНИЕ N СЕКУНД": в отличие от связки
 *     0.98→0.99 на BTC (где win rate в последние секунды был БЛИЗОК К
 *     порогу безубытка и требовал позднего входа), у 0.97→0.98 порог
 *     безубытка ниже (97%), а реальный win rate держится на 99%+ на
 *     ЛЮБОМ отрезке 5-минутного окна — даже за 120-300 сек до закрытия.
 *     Значит, ждать конца окна тут смысла нет: чем раньше вошли по
 *     0.97, тем больше шансов реально долететь до 0.98 и выйти с
 *     прибылью, а не зависнуть в резолве.
 *
 *  3. ПОТОЛОК ВХОДА (защита от "купил по 98 вместо 97"): не покупаем,
 *     если цена уже на уровне (тейк − MIN_PROFIT_MARGIN) или выше —
 *     иначе рискуем купить впритык к цели (или выше неё), и тогда
 *     реальной прибыли взять неоткуда: либо мгновенный слив в ноль,
 *     либо вынужденное ожидание резолва вместо быстрого выхода.
 *
 *  4. КВОТА СДЕЛОК В ЧАС: настраивается через Telegram командой
 *     "квота N". Как только за текущий час набрано N сделок — новые
 *     входы не ищутся до начала следующего часа.
 *
 *  ВХОД И ВЫХОД — РЫНОЧНЫМИ ОРДЕРАМИ через `ClobService.placeLimitOrder`
 *  (несмотря на название метода — по факту это FAK market-ордер,
 *  исполняется сразу целиком или отменяется, ничего не висит в стакане).
 *
 * ✅ ПОДТВЕРЖДЕНО НА ЖИВОЙ СДЕЛКЕ: making/taking семантика биржи — это
 *   "сколько ОТДАЛИ" / "сколько ПОЛУЧИЛИ", а не фиксированно
 *   "акции"/"доллары". При ПОКУПКЕ отдаём доллары, получаем акции — то
 *   есть для BUY поля resp.filledSize/resp.filledUsdc на самом деле
 *   содержат доллары/акции В ОБРАТНОМ порядке относительно своих
 *   названий. Для ПРОДАЖИ порядок совпадает с названиями полей. Код
 *   ниже (placeMarketOrder) меняет их местами именно для BUY.
 *
 * Стоп-лосс отсутствует — позиция держится до тейка (рыночным ордером,
 * как только цена дошла до тейка) или до официального резолва через
 * Gamma API, если тейк не успел сработать до закрытия окна.
 *
 * НАСТРОЙКИ НА ЛЕТУ ЧЕРЕЗ TELEGRAM (без передеплоя):
 *   цена 0.97     — цена входа
 *   тейк 0.98     — цена выхода (можно поставить 0.99)
 *   квота 2       — сколько сделок максимум за текущий час
 *   статус        — текущие настройки + что происходит сейчас
 *
 * DRY_RUN=true по умолчанию (FASTFLIP_DRY_RUN=false для реальных денег).
 * В DRY_RUN бот полностью симулирует сделку (вход → ожидание тейка или
 * резолва → выход), чтобы логику можно было проверить перед реальными
 * деньгами.
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

// Торгуем ВСЁ, КРОМЕ этой монеты (BTC уже показал себя хуже альтов на
// узких связках — см. анализ статистики).
const EXCLUDED_COIN = "Bitcoin";

const TARGET_WINDOW_MINUTES = 5;

const TIMEFRAMES_TO_DISCOVER = [{ suffixes: ["up-or-down-5m"], minutes: TARGET_WINDOW_MINUTES }];

const HOUR_MS = 60 * 60 * 1000;

// Раз в столько пересканируем список активных рынков (только когда нет
// открытой позиции и не идёт попытка входа — см. refreshMarkets).
const MARKET_REFRESH_MS = 15 * 1000;

// На сколько вперёд видим рынок, чтобы начать за ним следить.
function observeWindowMs(windowMinutes: number): number {
  return (windowMinutes + 1) * 60 * 1000;
}

// Минимальный технический запас времени перед закрытием, чтобы вообще
// пытаться войти — не стратегия, а просто защита от попытки купить в
// момент, когда физически может не успеть исполниться ордер.
const MIN_SEC_TO_CLOSE_FOR_ENTRY = 5;

// Буфер поверх живой цены стакана (в процентах) — передаётся в реальный
// метод ClobService.placeLimitOrder как maxSlippagePct. Он САМ берёт
// свежий стакан и целится в bestAsk*(1+X%) при покупке / bestBid*(1-X%)
// при продаже, с жёстким потолком/полом 0.999/0.001 в любом случае.
const MARKET_ORDER_SLIPPAGE_PCT = Number(process.env.FASTFLIP_SLIPPAGE_PCT ?? "0.5");

// Минимальный запас прибыли (в долях цены), без которого сделка не
// имеет смысла — используется в ДВУХ местах:
//   1. Потолок входа: не покупаем, если цена уже ≥ (тейк − этот запас).
//   2. Условие выхода: продаём только если цена ≥ (цена_покупки + этот
//      запас) — так реальное проскальзывание на исполнении (не на
//      входе, а прямо в момент сделки) тоже не даст продать в ноль.
const MIN_PROFIT_MARGIN = Number(process.env.FASTFLIP_MIN_PROFIT_MARGIN ?? "0.003");

// После закрытия окна ждём чуть-чуть (на случай гонки с последним тиком
// цены), и если тейк так и не сработал — идём резолвить через Gamma API.
const CLOSE_FALLBACK_BUFFER_MS = 5 * 1000;

const RESOLVE_CHECK_DELAY_SEC = 180;
const RESOLVE_RETRY_MS = 30 * 1000;
const RESOLVE_GIVE_UP_MS = 30 * 60 * 1000;

const REDEEM_POLL_MS = 60 * 1000;
const GAMMA_HOST = "https://gamma-api.polymarket.com";

// ─── Настройки, которые можно менять на лету через Telegram ───
const settings = {
  entryPrice: Number(process.env.FASTFLIP_ENTRY_PRICE ?? "0.97"),
  tpPrice: Number(process.env.FASTFLIP_TP_PRICE ?? "0.98"),
  quotaPerHour: Math.max(1, Number(process.env.FASTFLIP_QUOTA_PER_HOUR ?? "2")),
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
  exitAttemptInProgress: boolean;
  closed: boolean;
  // Таймер, который резолвит сделку через Gamma API, если тейк не
  // успел сработать до закрытия окна. Отменяем его, если тейк всё же
  // сработал раньше.
  resolveFallbackTimer: NodeJS.Timeout | null;
}

function buildTokenIndex(markets: CryptoUpDownMarket[]): Map<string, TokenInfo> {
  const idx = new Map<string, TokenInfo>();
  for (const m of markets) {
    idx.set(m.upTokenId, { market: m, side: "Up" });
    idx.set(m.downTokenId, { market: m, side: "Down" });
  }
  return idx;
}

/** Официальный резолв рынка через Gamma API (fallback-кейс, когда тейк не сработал по рынку). */
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

/** Результат попытки рыночного (FOK) ордера. filledSize=0 значит "не исполнился". */
interface MarketOrderResult {
  orderId: string | null;
  filledSize: number;
  avgPrice: number;
}

class FastFlipAltsBot {
  private watcher: PriceWatcher | null = null;
  private tokenIndex = new Map<string, TokenInfo>();
  private lastTokenIds: string[] = [];

  // Блокирует одновременный вход по нескольким токенам сразу (гонка,
  // если сетап сложился на двух рынках в одну и ту же секунду).
  private attemptInProgress = false;

  private openPosition: OpenPosition | null = null;
  private updateCount = 0;
  private tradesTotal = 0;

  // Квота в час.
  private currentHourKey: number | null = null;
  private tradesThisHour = 0;

  constructor(
    private clob: ClobService | null,
    private telegram: ReturnType<typeof createTelegramNotifier>,
  ) {}

  getStatus(): string {
    const watchedMarkets = this.tokenIndex.size / 2;
    return (
      `Режим: рыночные ордера (FOK), альткоины (без BTC)\n` +
      `Цена входа: ${settings.entryPrice} | Тейк: ${settings.tpPrice} | Потолок входа: не выше ${(settings.tpPrice - MIN_PROFIT_MARGIN).toFixed(3)}\n` +
      `Квота: ${this.tradesThisHour}/${settings.quotaPerHour} сделок в этом часе\n` +
      `Сейчас отслеживается активных 5-мин рынков: ${watchedMarkets}\n` +
      `Всего сделок с запуска: ${this.tradesTotal}\n` +
      `Открытая позиция: ${this.openPosition ? `${this.openPosition.market.title} (${this.openPosition.side})` : "нет"}`
    );
  }

  /** Сбрасывает счётчик квоты при смене часа. */
  private checkHourlyReset(): void {
    const hourKey = Math.floor(Date.now() / HOUR_MS);

    if (this.currentHourKey !== hourKey) {
      this.currentHourKey = hourKey;
      this.tradesThisHour = 0;
    }
  }

  /**
   * Пересканирует список активных НЕ-BTC 5-минутных рынков и обновляет
   * подписку PriceWatcher. Пока открыта позиция или идёт попытка входа —
   * НЕ трогаем существующую подписку.
   */
  async refreshMarkets(): Promise<void> {
    if (this.openPosition || this.attemptInProgress) return;

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
        m.coin.toUpperCase() !== EXCLUDED_COIN.toUpperCase() &&
        m.windowMinutes === TARGET_WINDOW_MINUTES &&
        m.closeTimeMs - now <= observeWindowMs(m.windowMinutes),
    );

    this.tokenIndex = buildTokenIndex(markets);
    const tokenIds = [...this.tokenIndex.keys()].sort();

    const coinsNow = [...new Set(markets.map((m) => m.coin))].sort();

    console.log(
      `[refresh] наблюдаем активных НЕ-BTC 5-мин рынков: ${markets.length} ` +
        `(${tokenIds.length} токенов, монеты: ${coinsNow.join(", ") || "нет"})`,
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

    // ── Позиция уже открыта — проверяем не пора ли выходить по тейку ──
    if (this.openPosition) {
      this.maybeTriggerExit(this.openPosition, market, price);
      return;
    }

    // ── Ищем вход ──
    if (this.attemptInProgress) return;

    this.checkHourlyReset();

    if (this.tradesThisHour >= settings.quotaPerHour) return; // квота часа выполнена

    const secToClose = (market.closeTimeMs - Date.now()) / 1000;

    // В отличие от версии для BTC, тут НЕТ ограничения "только последние
    // N секунд" — статистика показала, что win rate у связки 0.97→0.98
    // не проседает даже при входе за 120-300 сек до закрытия. Ждать
    // конца окна тут не нужно, чем раньше вошли — тем больше времени
    // реально долететь до тейка. Только минимальный технический запас,
    // чтобы ордер физически успел исполниться.
    if (secToClose < MIN_SEC_TO_CLOSE_FOR_ENTRY) return;

    if (price < settings.entryPrice) return;

    // Потолок входа: если цена УЖЕ на уровне тейка (или выше) — реальной
    // прибыли на этой сделке взять неоткуда. Именно это защищает от
    // "купил по 0.98 вместо 0.97" — вместо покупки вслепую просто
    // пропускаем эту конкретную свечу.
    const entryCeiling = settings.tpPrice - MIN_PROFIT_MARGIN;

    if (price > entryCeiling) return;

    this.attemptInProgress = true;
    const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
    this.executeMarketEntry(market, side, tokenId, price, secToClose);
  }

  private maybeTriggerExit(pos: OpenPosition, market: CryptoUpDownMarket, price: number): void {
    if (pos.closed || pos.exitAttemptInProgress) return;
    // Тик пришёл не по тому рынку, где у нас открыта позиция — игнор.
    if (market.eventSlug !== pos.market.eventSlug) return;

    // Реальная цель выхода: не просто "цена дошла до номинального
    // тейка", а "цена дошла до тейка И это даёт реальную прибыль сверх
    // того, что мы заплатили при покупке". Если проскальзывание на
    // входе (или сама покупка) уже съело весь запас — ждём резолва
    // вместо бессмысленного выхода в ноль/убыток.
    const effectiveExitPrice = Math.max(settings.tpPrice, pos.buyPrice + MIN_PROFIT_MARGIN);

    if (price < effectiveExitPrice) return;

    pos.exitAttemptInProgress = true;
    this.executeMarketExit(pos, price);
  }

  /** Единая точка вызова рыночного ордера — с реальным клобом или в DRY_RUN-симуляции. */
  private async placeMarketOrder(params: {
    tokenId: string;
    side: "BUY" | "SELL";
    size: number;
    nominalPrice: number; // используется методом только как fallback, если стакан вдруг пуст
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

      // making/taking семантика биржи — "сколько ОТДАЛИ" / "сколько
      // ПОЛУЧИЛИ". При BUY отдаём доллары, получаем акции — то есть
      // resp.filledSize/resp.filledUsdc для BUY содержат доллары/акции
      // В ОБРАТНОМ порядке относительно своих названий. Для SELL порядок
      // совпадает с названиями полей.
      const rawA = Number(resp.filledSize ?? 0);
      const rawB = Number(resp.filledUsdc ?? 0);

      const filledShares = params.side === "BUY" ? rawB : rawA;
      const filledDollars = params.side === "BUY" ? rawA : rawB;

      const avgPrice = filledShares > 0 ? filledDollars / filledShares : 0;

      return { orderId: null, filledSize: filledShares, avgPrice };
    } catch (err) {
      console.log(`   ⏳ Рыночный ордер (${params.side}) не исполнился: ${(err as Error).message}`);
      return { orderId: null, filledSize: 0, avgPrice: 0 };
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
      `\n⚡ ВХОД ПО РЫНКУ: [${market.coin} / 5мин] "${market.title}"\n` +
        `   Сторона: ${side} | Цена сейчас: ~${priceAtEntry} | До закрытия: ${secToClose.toFixed(1)}с\n` +
        `   Покупаем: ${size.toFixed(2)} акций рыночным ордером (цель ~${settings.entryPrice}, ~$${TRADE_SIZE_USD})`,
    );

    const result = await this.placeMarketOrder({ tokenId, side: "BUY", size, nominalPrice: settings.entryPrice });

    if (result.filledSize <= 0) {
      console.log(`   ⏳ Рыночная покупка не исполнилась (eventSlug: ${market.eventSlug}). Продолжаю мониторинг.`);
      this.attemptInProgress = false;
      return;
    }

    console.log(`   💰 ПОКУПКА ИСПОЛНЕНА ПО РЫНКУ: ${result.filledSize.toFixed(2)} акций по ~${result.avgPrice}.`);

    const pos: OpenPosition = {
      market,
      side,
      tokenId,
      buyPrice: result.avgPrice,
      filledSize: result.filledSize,
      exitAttemptInProgress: false,
      closed: false,
      resolveFallbackTimer: null,
    };
    this.openPosition = pos;

    // Если фактическое исполнение всё же съело запас прибыли (несмотря
    // на потолок входа — цена могла дёрнуться между проверкой и
    // реальным исполнением ордера) — предупреждаем прямо сейчас.
    const effectiveExitPrice = Math.max(settings.tpPrice, pos.buyPrice + MIN_PROFIT_MARGIN);
    const slippageAteMargin = effectiveExitPrice > settings.tpPrice + 0.0001;

    if (this.telegram) {
      const warning = slippageAteMargin
        ? `\n⚠️ Проскальзывание при исполнении съело запас прибыли (купили по ${result.avgPrice.toFixed(3)}, номинальный тейк ${settings.tpPrice}) — мгновенный выход отменён, жду реального роста цены или резолва.`
        : `\nЖду тейк ${settings.tpPrice} по рынку, либо закрытия окна...`;

      await this.telegram.send(
        `💰 Куплено по рынку: ${market.title}\nСторона: ${side}\nЦена: ${result.avgPrice.toFixed(3)} | Размер: ${result.filledSize.toFixed(2)}${warning}`,
      );
    }

    // Если тейк не сработает до закрытия окна — идём резолвить через Gamma API.
    const msUntilCloseCheck = Math.max(0, market.closeTimeMs - Date.now() + CLOSE_FALLBACK_BUFFER_MS);
    pos.resolveFallbackTimer = setTimeout(() => {
      if (pos.closed) return;
      console.log(`   ⏳ Тейк не сработал до закрытия — жду официальный резолв (eventSlug: ${market.eventSlug}).`);
      this.scheduleResolveFallback(pos);
    }, msUntilCloseCheck);
  }

  private async executeMarketExit(pos: OpenPosition, priceAtExit: number): Promise<void> {
    console.log(
      `\n🎯 ВЫХОД ПО РЫНКУ: [${pos.market.coin} / 5мин] "${pos.market.title}"\n` +
        `   Сторона: ${pos.side} | Цена сейчас: ~${priceAtExit} | Продаём: ${pos.filledSize.toFixed(2)} акций рыночным ордером (цель ~${settings.tpPrice})`,
    );

    const result = await this.placeMarketOrder({
      tokenId: pos.tokenId,
      side: "SELL",
      size: pos.filledSize,
      nominalPrice: settings.tpPrice,
    });

    if (result.filledSize < pos.filledSize - 0.001) {
      console.log(
        `   ⚠️ Рыночная продажа не прошла целиком (${result.filledSize.toFixed(2)}/${pos.filledSize.toFixed(2)}). Пробую снова при следующем касании тейка.`,
      );
      pos.exitAttemptInProgress = false;
      return;
    }

    if (pos.resolveFallbackTimer) {
      clearTimeout(pos.resolveFallbackTimer);
      pos.resolveFallbackTimer = null;
    }

    pos.closed = true;
    const profit = result.filledSize * (result.avgPrice - pos.buyPrice);
    await this.notifyClose(pos.market, pos.side, "тейк по рынку", "WIN", profit);
    this.finishTrade(pos);
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
      await this.notifyClose(pos.market, pos.side, "резолв рынка", outcome, profit);
      this.finishTrade(pos);
    };
    setTimeout(check, RESOLVE_RETRY_MS);
  }

  /** Вызывается после любого способа закрытия сделки — обнуляет позицию, засчитывает в квоту часа и возвращается к мониторингу. */
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
      `✅ Сделка завершена (всего с запуска: ${this.tradesTotal}, в этом часе: ${this.tradesThisHour}/${settings.quotaPerHour}). Возвращаюсь к мониторингу.`,
    );

    this.refreshMarkets().catch((err) => console.error("[refresh] ошибка:", (err as Error).message));
  }

  private async notifyClose(
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    how: string,
    outcome: "WIN" | "LOSS",
    profit: number,
  ): Promise<void> {
    const sign = outcome === "WIN" ? "✅ ПРИБЫЛЬ" : "🔻 УБЫТОК";
    const msg =
      `${sign} (${outcome})\n` +
      `Способ закрытия: ${how}\n` +
      `${market.title}\n` +
      `Сторона: ${side}\n` +
      `Профит: ${profit >= 0 ? "+" : ""}$${profit.toFixed(3)}`;
    console.log(`\n${msg}\n`);
    if (this.telegram) await this.telegram.send(msg);
  }

  start(): void {
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
  bot: FastFlipAltsBot,
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
          await telegram?.send(`Цена входа установлена: ${settings.entryPrice}`);
          continue;
        }

        const tpMatch = text.match(/^тейк\s+([\d.]+)$/);
        if (tpMatch) {
          settings.tpPrice = Number(tpMatch[1]);
          await telegram?.send(`Тейк-профит установлен: ${settings.tpPrice}`);
          continue;
        }

        const quotaMatch = text.match(/^квота\s+(\d+)$/);
        if (quotaMatch) {
          settings.quotaPerHour = Math.max(1, Number(quotaMatch[1]));
          await telegram?.send(`Квота установлена: ${settings.quotaPerHour} сделок в час`);
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
  console.log(`Режим: ${DRY_RUN ? "DRY_RUN (без реальных сделок, полная симуляция)" : "⚠️  LIVE — РЕАЛЬНЫЕ ДЕНЬГИ"}`);
  console.log(
    `Актив: все монеты кроме BTC | Вход: ${settings.entryPrice} (рынком, без ограничения по времени) | ` +
      `Тейк: ${settings.tpPrice} (рынком) | Квота: ${settings.quotaPerHour}/час | Стоп-лосс на сделку: убран`,
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

  const bot = new FastFlipAltsBot(clob, telegram);
  bot.start();

  if (telegram && process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID) {
    console.log("Telegram-команды включены: цена X / тейк X / квота X / статус");
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