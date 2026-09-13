/**
 * "Быстрый флип" v6.0 — РЫНОЧНЫЕ ордера на ВХОД, БЕЗ ПРОДАЖИ В ПЛЮС —
 * ПОЗИЦИЯ ДЕРЖИТСЯ ДО ОФИЦИАЛЬНОГО РЕЗОЛВА. СТОП-ЛОСС по рынку остаётся
 * как единственный вариант досрочного выхода. ЖЁСТКИЙ ПОТОЛОК ЦЕНЫ
 * ПОКУПКИ (не выше maxEntryPrice ни при каких раскладах).
 *
 * ИЗМЕНЕНИЯ В v6.0 (относительно v5.6) — ПО ПРОСЬБЕ: "бот покупает по
 * 97-98 и продаёт по 99, сделай так, чтобы стоял до резолва, а не
 * продавал":
 *
 *   - Полностью убран висящий лимитный ордер на тейк-профит (GTC),
 *     который раньше выставлялся сразу после покупки
 *     (placeTpLimitWithRetry / startTpPolling / finalizeTpBeforeOverride
 *     / parseGtcOrderStatus — всё удалено, как и поле settings.tpPrice
 *     больше нигде не влияет на выход).
 *   - Убран и тик-триггер тейка, который раньше работал в DRY_RUN
 *     (executeMarketExit) — теперь в любом режиме (DRY_RUN и LIVE)
 *     позиция НЕ продаётся по достижении какой-либо "прибыльной" цены.
 *   - После покупки бот просто ждёт: либо сработает СТОП-ЛОСС (если
 *     settings.slPrice > 0 и цена упала до него или ниже — это
 *     единственный оставшийся вариант досрочного выхода по рынку),
 *     либо доходим до закрытия 5-минутного окна и уходим на официальный
 *     резолв через Gamma API (resolveWinner), как раньше.
 *   - Команда Telegram "тейк X" оставлена в коде (чтобы не ломать
 *     остальной интерфейс), но она больше ни на что не влияет —
 *     сама settings.tpPrice не используется никаким выходом.
 *
 * ВСЁ ОСТАЛЬНОЕ — БЕЗ ИЗМЕНЕНИЙ ОТНОСИТЕЛЬНО v5.6:
 *
 *   1. Жёсткий потолок покупки — проверка ПОСЛЕ реального исполнения
 *      (не только по цене тика до отправки), см. isBadFillHigh.
 *   2. Аварийный выход, если реальная цена покупки оказалась намного
 *      выше потолка или намного ниже ожидаемой (см. emergencyExit).
 *   3. Стоп-лосс — АБСОЛЮТНАЯ цена токена (0.0–1.0), а не доля от цены
 *      входа. По умолчанию 0.90. Поставь 0, чтобы отключить стоп
 *      полностью — тогда бот будет держать позицию только до резолва,
 *      без каких-либо досрочных выходов вообще.
 *
 * DRY_RUN=true по умолчанию (FASTFLIP_DRY_RUN=false для реальных денег).
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

const TIMEFRAMES_TO_DISCOVER = [{ suffixes: ["up-or-down-5m"], minutes: TARGET_WINDOW_MINUTES }];

const HOUR_MS = 60 * 60 * 1000;

// Раз в столько пересканируем список активных рынков (только когда нет
// открытой позиции, не идёт попытка входа И ни один отслеживаемый
// рынок не в критическом окне — см. refreshMarkets / hasCriticalMarket).
const MARKET_REFRESH_MS = 15 * 1000;

const REFRESH_SAFETY_BUFFER_SEC = 30;

function observeWindowMs(windowMinutes: number): number {
  return (windowMinutes + 1) * 60 * 1000;
}

// Буфер поверх живой цены стакана (в процентах) — передаётся в реальный
// метод ClobService.placeLimitOrder как maxSlippagePct.
const MARKET_ORDER_SLIPPAGE_PCT = Number(process.env.FASTFLIP_SLIPPAGE_PCT ?? "0.5");

// Если реальная цена покупки оказалась НАМНОГО ниже ожидаемой (entryPrice)
// — значит цена рухнула ПРЯМО ПОКА ордер летел до биржи. В этом случае
// сразу пробуем аварийно продать обратно вместо того, чтобы слепо
// держать до резолва — см. executeMarketEntry/emergencyExit.
const BAD_FILL_TOLERANCE = Number(process.env.FASTFLIP_BAD_FILL_TOLERANCE ?? "0.02");

// Допуск на овершут реальной цены исполнения покупки сверх maxEntryPrice
// (тонкий стакан в последние секунды перед закрытием окна).
const MAX_ENTRY_OVERSHOOT_TOLERANCE = Number(process.env.FASTFLIP_MAX_ENTRY_OVERSHOOT_TOLERANCE ?? "0.008");

// После закрытия окна ждём чуть-чуть (на случай гонки с последним тиком
// цены / стопом), и если стоп так и не сработал — идём резолвить через
// Gamma API.
const CLOSE_FALLBACK_BUFFER_MS = 5 * 1000;

const RESOLVE_CHECK_DELAY_SEC = 180;
const RESOLVE_RETRY_MS = 30 * 1000;
const RESOLVE_GIVE_UP_MS = 30 * 60 * 1000;

const REDEEM_POLL_MS = 60 * 1000;
const GAMMA_HOST = "https://gamma-api.polymarket.com";

// ─── Настройки, которые можно менять на лету через Telegram ───
const settings = {
  // Нижняя граница коридора входа.
  entryPrice: Number(process.env.FASTFLIP_ENTRY_PRICE ?? "0.97"),
  // ВЕРХНЯЯ граница коридора входа. Если цена уже выше — сетап
  // считается "проехавшим", вход не совершается. Это ещё и жёсткий
  // потолок на РЕАЛЬНУЮ цену исполнения покупки, см.
  // MAX_ENTRY_OVERSHOOT_TOLERANCE.
  maxEntryPrice: Number(process.env.FASTFLIP_MAX_ENTRY_PRICE ?? "0.98"),
  // v6.0: больше НИ НА ЧТО не влияет — оставлено только чтобы не
  // ломать команду Telegram "тейк X", если она где-то используется в
  // твоих заметках/привычке. Никакой продажи по этой цене больше нет.
  tpPrice: Number(process.env.FASTFLIP_TP_PRICE ?? "0.99"),
  // Стоп-лосс: АБСОЛЮТНАЯ цена токена (0.0–1.0), а не доля от цены
  // входа. Если цена после входа падает ДО этого уровня ИЛИ НИЖЕ —
  // бот немедленно пытается выйти по рынку, не дожидаясь резолва.
  // Поставь 0, чтобы отключить стоп — тогда бот держит ВСЕГДА до резолва.
  slPrice: Number(process.env.FASTFLIP_SL_PRICE ?? "0.90"),
  // Сколько секунд до закрытия текущей 5-минутки ещё разрешён вход.
  entryWindowSec: Number(process.env.FASTFLIP_ENTRY_WINDOW_SEC ?? "60"),
  // Большое число по умолчанию = фактически без ограничения, пока не
  // задано явно через env или команду "квота N".
  quotaPerHour: Math.max(1, Number(process.env.FASTFLIP_QUOTA_PER_HOUR ?? "999")),
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
  // Минимальная цена, зафиксированная с момента входа — нужна для
  // расчёта максимальной просадки сделки, которую пришлём в отчёте.
  minPriceSinceEntry: number;
  // Таймер, который резолвит сделку через Gamma API, если стоп не
  // успел сработать до закрытия окна. Отменяем его, если стоп всё же
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

/** Официальный резолв рынка через Gamma API. */
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

/**
 * Самопроверяющийся разбор ответа биржи на предмет "что тут акции, а
 * что доллары" — используется для рыночных (taker) ордеров (вход,
 * стоп-лосс, аварийный выход).
 */
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

  constructor(
    private clob: ClobService | null,
    private telegram: ReturnType<typeof createTelegramNotifier>,
  ) {}

  getStatus(): string {
    const watchedMarkets = this.tokenIndex.size / 2;
    return (
      `Режим: рыночный вход, БЕЗ продажи в плюс — держим до резолва, BTC\n` +
      `Вход: ${settings.entryPrice}–${settings.maxEntryPrice} | Стоп: ${settings.slPrice} | Окно входа: последние ${settings.entryWindowSec}с\n` +
      `Допуск на овершут входа: ${MAX_ENTRY_OVERSHOOT_TOLERANCE}\n` +
      `Квота: ${this.tradesThisHour}/${settings.quotaPerHour} сделок в этом часе\n` +
      `Сейчас отслеживается активных 5-мин рынков: ${watchedMarkets}\n` +
      `Всего сделок с запуска: ${this.tradesTotal}\n` +
      `Открытая позиция: ${
        this.openPosition
          ? `${this.openPosition.market.title} (${this.openPosition.side}), цена входа ${this.openPosition.buyPrice.toFixed(4)}, держим до резолва (стоп: ${settings.slPrice > 0 ? settings.slPrice : "выключен"})`
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

    // ── Позиция уже открыта — обновляем минимум цены (для просадки в
    // отчёте) и проверяем не пора ли аварийно выходить по стопу. Тейка
    // больше нет — держим до резолва ──
    if (this.openPosition) {
      this.maybeTriggerStop(this.openPosition, market, price);
      return;
    }

    if (this.attemptInProgress) return;

    this.checkHourlyReset();

    if (this.tradesThisHour >= settings.quotaPerHour) return; // квота часа выполнена

    const secToClose = (market.closeTimeMs - Date.now()) / 1000;

    if (secToClose > settings.entryWindowSec || secToClose < 0) return;

    if (price < settings.entryPrice) return;
    if (price > settings.maxEntryPrice) return;

    this.attemptInProgress = true;
    const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
    this.executeMarketEntry(market, side, tokenId, price, secToClose);
  }

  /**
   * Вызывается на каждый тик цены, пока позиция открыта. v6.0: больше
   * НЕТ проверки тейка — единственная причина досрочного выхода это
   * стоп-лосс. Если стоп выключен (slPrice = 0) — эта функция вообще
   * ничего не делает, кроме отслеживания минимальной цены (для отчёта
   * о просадке), и позиция держится до резолва в любом случае.
   */
  private maybeTriggerStop(pos: OpenPosition, market: CryptoUpDownMarket, price: number): void {
    if (market.eventSlug !== pos.market.eventSlug) return;

    if (price < pos.minPriceSinceEntry) {
      pos.minPriceSinceEntry = price;
    }

    if (pos.closed || pos.exitAttemptInProgress) return;

    // Единственный оставшийся вариант досрочного выхода — стоп-лосс.
    // settings.slPrice — АБСОЛЮТНАЯ цена токена (0.0-1.0), а не доля
    // от цены входа.
    if (settings.slPrice > 0 && price <= settings.slPrice) {
      pos.exitAttemptInProgress = true;
      this.executeStopLoss(pos, price);
    }
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
      exitAttemptInProgress: false,
      closed: false,
      minPriceSinceEntry: result.avgPrice,
      resolveFallbackTimer: null,
    };
    this.openPosition = pos;

    const isBadFillLow = pos.buyPrice < settings.entryPrice - BAD_FILL_TOLERANCE;
    const isBadFillHigh = pos.buyPrice > settings.maxEntryPrice + MAX_ENTRY_OVERSHOOT_TOLERANCE;

    if (isBadFillLow || isBadFillHigh) {
      const reason = isBadFillHigh
        ? `купили ДОРОЖЕ потолка коридора: ${pos.buyPrice.toFixed(5)} (потолок ${settings.maxEntryPrice} + допуск ${MAX_ENTRY_OVERSHOOT_TOLERANCE} = ${(settings.maxEntryPrice + MAX_ENTRY_OVERSHOOT_TOLERANCE).toFixed(5)}, превышение на ${(pos.buyPrice - settings.maxEntryPrice - MAX_ENTRY_OVERSHOOT_TOLERANCE).toFixed(5)})`
        : `цена рухнула ПРЯМО во время исполнения ордера: купили по ${pos.buyPrice.toFixed(5)} вместо ожидаемых ~${priceAtEntry.toFixed(5)} (допуск ${BAD_FILL_TOLERANCE})`;

      console.log(`   🚨 Плохой филл на входе: ${reason}. Пробую аварийно продать обратно прямо сейчас.`);
      if (this.telegram) {
        await this.telegram.send(`🚨 Плохой филл на входе: ${reason}. Пробую аварийно продать обратно прямо сейчас.`);
      }

      this.emergencyExit(pos);
      return;
    }

    if (settings.slPrice > 0 && pos.buyPrice <= settings.slPrice) {
      console.log(
        `   ⚠️ Цена покупки (${pos.buyPrice.toFixed(3)}) уже на уровне стопа (${settings.slPrice}) или ниже — выход сработает на следующем тике.`,
      );
    }

    if (this.telegram) {
      const holdNote = settings.slPrice > 0
        ? `\nДержим до официального резолва. Стоп-лосс активен: ${settings.slPrice}.`
        : `\nДержим до официального резолва. Стоп-лосс выключен — досрочного выхода не будет.`;

      await this.telegram.send(
        `💰 Куплено по рынку: ${market.title}\nСторона: ${side}\nЦена: ${result.avgPrice.toFixed(4)} | Размер: ${result.filledSize.toFixed(2)}${holdNote}`,
      );
    }

    const msUntilCloseCheck = Math.max(0, market.closeTimeMs - Date.now() + CLOSE_FALLBACK_BUFFER_MS);
    pos.resolveFallbackTimer = setTimeout(() => {
      if (pos.closed) return;
      console.log(`   ⏳ Окно закрылось, стоп не сработал — жду официальный резолв (eventSlug: ${market.eventSlug}).`);
      this.scheduleResolveFallback(pos);
    }, msUntilCloseCheck);
  }

  private async emergencyExit(pos: OpenPosition): Promise<void> {
    console.log(
      `\n🚨 АВАРИЙНЫЙ ВЫХОД: [BTC / 5мин] "${pos.market.title}" — плохой филл на входе, пробую продать немедленно.`,
    );

    const result = await this.placeMarketOrder({
      tokenId: pos.tokenId,
      side: "SELL",
      size: pos.filledSize,
      nominalPrice: pos.buyPrice,
    });

    if (result.filledSize >= pos.filledSize - 0.001) {
      pos.closed = true;
      const profit = result.filledSize * (result.avgPrice - pos.buyPrice);
      const outcome: "WIN" | "LOSS" = profit >= 0 ? "WIN" : "LOSS";
      await this.notifyClose(pos, "аварийный выход (плохой филл на входе)", outcome, profit);
      this.finishTrade(pos);
      return;
    }

    console.log(`   ⚠️ Аварийная продажа не прошла — держим до стопа/резолва как обычно.`);

    const msUntilCloseCheck = Math.max(0, pos.market.closeTimeMs - Date.now() + CLOSE_FALLBACK_BUFFER_MS);
    pos.resolveFallbackTimer = setTimeout(() => {
      if (pos.closed) return;
      console.log(`   ⏳ Аварийный выход не удался — жду официальный резолв (eventSlug: ${pos.market.eventSlug}).`);
      this.scheduleResolveFallback(pos);
    }, msUntilCloseCheck);
  }

  private async executeStopLoss(pos: OpenPosition, priceAtExit: number): Promise<void> {
    console.log(
      `\n🛑 СТОП-ЛОСС: [BTC / 5мин] "${pos.market.title}"\n` +
        `   Сторона: ${pos.side} | Цена сейчас: ~${priceAtExit} (порог стопа: ${settings.slPrice}) | ` +
        `Продаём: ${pos.filledSize.toFixed(2)} акций рыночным ордером НЕМЕДЛЕННО`,
    );

    const result = await this.placeMarketOrder({
      tokenId: pos.tokenId,
      side: "SELL",
      size: pos.filledSize,
      nominalPrice: priceAtExit,
    });

    if (result.filledSize < pos.filledSize - 0.001) {
      console.log(
        `   ⚠️ Стоп не исполнился целиком (${result.filledSize.toFixed(2)}/${pos.filledSize.toFixed(2)}). Пробую снова при следующем тике.`,
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
    const outcome: "WIN" | "LOSS" = profit >= 0 ? "WIN" : "LOSS";
    await this.notifyClose(pos, "стоп-лосс", outcome, profit);
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

        const tpMatch = text.match(/^тейк\s+([\d.]+)$/);
        if (tpMatch) {
          settings.tpPrice = Number(tpMatch[1]);
          await telegram?.send(`⚠️ Значение сохранено (${settings.tpPrice}), но тейк-выход отключён в этой версии — бот держит позиции до резолва.`);
          continue;
        }

        const slMatch = text.match(/^стоп\s+([\d.]+)$/);
        if (slMatch) {
          settings.slPrice = Number(slMatch[1]);
          await telegram?.send(
            `Стоп-лосс установлен: ${settings.slPrice} (0 = выключен). Это АБСОЛЮТНАЯ цена токена (0-1), а не доля от цены входа. При падении цены до этого уровня или ниже — немедленный выход по рынку.`,
          );
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
    `Актив: BTC only | Вход: ${settings.entryPrice}-${settings.maxEntryPrice} (рынком, окно ${settings.entryWindowSec}с) | ` +
      `Тейк: ОТКЛЮЧЁН — держим до резолва | Стоп: ${settings.slPrice} (рынком) | Квота: ${settings.quotaPerHour}/час | ` +
      `Допуск овершута входа: ${MAX_ENTRY_OVERSHOOT_TOLERANCE}`,
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
    console.log("Telegram-команды включены: цена X / цена_макс X / стоп X / окно X / квота X / статус");
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