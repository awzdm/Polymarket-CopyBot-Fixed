/**
 * "Быстрый флип" v7.0 — РЫНОЧНЫЕ ордера на ВХОД, держим ДО ОФИЦИАЛЬНОГО
 * РЕЗОЛВА. Никаких лимиток на выход и никаких продаж по тику цены.
 *
 * v7.0 (относительно v6.8): ДОБАВЛЕНА ПОДДЕРЖКА НЕСКОЛЬКИХ ПРОФИЛЕЙ ВХОДА.
 *
 * Раньше был один-единственный набор настроек (окно входа + пороги
 * движения по монетам). Теперь можно держать НЕСКОЛЬКО таких наборов
 * одновременно ("профилей") — бот на каждом апдейте цены проверяет ИХ ВСЕ
 * по очереди, и если хотя бы ОДИН профиль полностью совпал (окно + порог
 * движения монеты + коридор цены токена — коридор общий на все профили),
 * бот входит. Это НЕ "или-или" в смысле выбора одной стратегии — это
 * буквально "любой профиль может дать сигнал", то есть суммарно сделок
 * должно стать больше, а не просто по-другому фильтроваться те же самые.
 *
 * Сделка всё ещё одна за раз (openPosition) — профили не открывают
 * параллельные позиции, они просто расширяют условия, при которых боту
 * разрешено войти в СЛЕДУЮЩУЮ сделку.
 *
 * По умолчанию заведено 2 профиля:
 *   Профиль 1 (основной): окно 90с, пороги по монетам как раньше
 *   Профиль 2 (новый):    окно 60с, порог 0.13% для всех монет
 *
 * Управление профилями через Telegram:
 *   профиль <N> окно <секунды>
 *   профиль <N> движение <монета> <значение>
 *   профиль <N> движение <значение>              (всем монетам профиля N разом)
 * Старые команды без "профиль N" (окно X / движение ...) по-прежнему
 * работают и адресуются ПРОФИЛЮ 1 — для обратной совместимости с
 * привычками управления.
 */

import "dotenv/config";
import { Side } from "@polymarket/clob-client-v2";
import { discoverCryptoUpDownMarkets, CryptoUpDownMarket } from "./cryptoMarketDiscovery.js";
import { PriceWatcher, PriceUpdate } from "./priceWatcher.js";
import { btcPriceFeed } from "./btcPriceFeed.js";
import { ethPriceFeed } from "./ethPriceFeed.js";
import { solPriceFeed } from "./solPriceFeed.js";
import { xrpPriceFeed } from "./xrpPriceFeed.js";
import { dogePriceFeed } from "./dogePriceFeed.js";
import { ClobService } from "./clob.js";
import { userStream } from "./userStream.js";
import { RedeemService } from "./redeem.js";
import { DataApiClient } from "./dataApi.js";
import { createTelegramNotifier } from "./telegram.js";
import { createLogger } from "./logger.js";

const DRY_RUN = (process.env.FASTFLIP_DRY_RUN ?? "true").toLowerCase() !== "false";
const AUTO_REDEEM = (process.env.FASTFLIP_AUTO_REDEEM ?? "true").toLowerCase() !== "false";
const TRADE_SIZE_USD = Number(process.env.FASTFLIP_TRADE_SIZE_USD ?? "5");

// Список торгуемых монет. Каждая монета обязана иметь свой фид в
// PRICE_FEEDS ниже и свой порог движения в каждом профиле.
const COINS = ["Bitcoin", "Ethereum", "Solana", "XRP", "Dogecoin"];
const TARGET_WINDOW_MINUTES = 5;

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

const TRADE_CONFIRMATION_TIMEOUT_MS = 60 * 1000;

const GAMMA_HOST = "https://gamma-api.polymarket.com";
const REDEEM_POLL_MS = 60 * 1000;

// Соответствие монета -> её фид цены. Все фиды запускаются в main().
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

// Пороги движения ПРОФИЛЯ 1 (основного) — те же, что были раньше единственными.
const PROFILE_1_DEFAULT_THRESHOLDS: Record<string, number> = {
  Bitcoin: 0.0013,
  Ethereum: 0.0014,
  Solana: 0.005,
  XRP: 0.0015,
  Dogecoin: 0.0027,
};

// ─── Профили входа ───
// Профиль — это самостоятельный набор (окно входа + пороги по монетам).
// Коридор цены токена (entryPrice/maxEntryPrice) ОБЩИЙ на все профили —
// его не имеет смысла разводить по профилям, это не про движение монеты,
// а про то, насколько рынок уже уверен в исходе.
interface EntryProfile {
  name: string;
  entryWindowSec: number;
  pctMoveThresholds: Record<string, number>;
}

function makeUniformThresholds(value: number): Record<string, number> {
  const out: Record<string, number> = {};
  for (const c of COINS) out[c] = value;
  return out;
}

// ─── Настройки, которые можно менять на лету через Telegram ───
const settings = {
  entryPrice: Number(process.env.FASTFLIP_ENTRY_PRICE ?? "0.97"),
  maxEntryPrice: Number(process.env.FASTFLIP_MAX_ENTRY_PRICE ?? "0.98"),
  // Квота фактически снята — 999 в час не является реальным ограничением,
  // частоту сделок регулируют только профили.
  quotaPerHour: Math.max(1, Number(process.env.FASTFLIP_QUOTA_PER_HOUR ?? "999")),
  profiles: [
    {
      name: "Профиль 1 (основной)",
      entryWindowSec: Number(process.env.FASTFLIP_ENTRY_WINDOW_SEC ?? "90"),
      pctMoveThresholds: { ...PROFILE_1_DEFAULT_THRESHOLDS },
    },
    {
      name: "Профиль 2",
      entryWindowSec: 60,
      pctMoveThresholds: makeUniformThresholds(0.0013), // 0.13% всем монетам
    },
  ] as EntryProfile[],
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
}

function buildTokenIndex(markets: CryptoUpDownMarket[]): Map<string, TokenInfo> {
  const idx = new Map<string, TokenInfo>();
  for (const m of markets) {
    idx.set(m.upTokenId, { market: m, side: "Up" });
    idx.set(m.downTokenId, { market: m, side: "Down" });
  }
  return idx;
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

  // зафиксированная цена монеты на момент открытия окна, по eventSlug
  private openPrices: Map<string, number> = new Map();

  constructor(
    private clob: ClobService | null,
    private telegram: ReturnType<typeof createTelegramNotifier>,
  ) {}

  private maxEntryWindowSec(): number {
    return Math.max(...settings.profiles.map((p) => p.entryWindowSec));
  }

  getStatus(): string {
    const watchedMarkets = this.tokenIndex.size / 2;
    const profilesLines = settings.profiles
      .map(
        (p, i) =>
          `  [${i + 1}] ${p.name} — окно ${p.entryWindowSec}с, пороги: ` +
          COINS.map((c) => `${c} ${((p.pctMoveThresholds[c] ?? 0) * 100).toFixed(2)}%`).join(", "),
      )
      .join("\n");
    return (
      `Режим: рыночный вход, держим ДО РЕЗОЛВА (без выхода по лимитке), монеты: ${COINS.join(", ")}\n` +
      `Коридор входа (общий на все профили): ${settings.entryPrice}–${settings.maxEntryPrice}\n` +
      `Профили входа (сигнал ЛЮБОГО из них достаточен для входа):\n${profilesLines}\n` +
      `Квота: ${this.tradesThisHour}/${settings.quotaPerHour} сделок в этом часе\n` +
      `Сейчас отслеживается активных 5-мин рынков: ${watchedMarkets}\n` +
      `Всего сделок с запуска: ${this.tradesTotal}\n` +
      `Открытая позиция: ${
        this.openPosition
          ? `${this.openPosition.market.title} (${this.openPosition.side}), цена входа ${this.openPosition.buyPrice.toFixed(4)}, держим до резолва`
          : "нет"
      }`
    );
  }

  private checkHourlyReset(): void {
    const hourKey = Math.floor(Date.now() / HOUR_MS);
    if (this.currentHourKey !== hourKey) {
      this.currentHourKey = hourKey;
      this.tradesThisHour = 0;
      console.log(`[час] Новый час — квота сброшена (0/${settings.quotaPerHour}).`);
    }
  }

  private hasCriticalMarket(): boolean {
    const now = Date.now();
    const criticalMs = (this.maxEntryWindowSec() + REFRESH_SAFETY_BUFFER_SEC) * 1000;
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

    const allowedCoins = new Set(COINS.map((c) => c.toUpperCase()));
    const now = Date.now();
    const markets = allMarkets.filter(
      (m) =>
        allowedCoins.has(m.coin.toUpperCase()) &&
        m.windowMinutes === TARGET_WINDOW_MINUTES &&
        m.closeTimeMs - now <= observeWindowMs(m.windowMinutes),
    );

    this.tokenIndex = buildTokenIndex(markets);
    const tokenIds = [...this.tokenIndex.keys()].sort();

    console.log(`[refresh] наблюдаем активных рынков (${COINS.join("/")}) 5-мин: ${markets.length} (${tokenIds.length} токенов)`);

    // чистим сохранённые цены открытия для рынков, которых больше нет в наблюдении
    const activeSlugs = new Set(markets.map((m) => m.eventSlug));
    for (const slug of this.openPrices.keys()) {
      if (!activeSlugs.has(slug)) this.openPrices.delete(slug);
    }

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

    // Позиция уже открыта — никакой реакции на цену, кроме отслеживания
    // минимума (для статистики просадки в отчёте о резолве). Закрытие
    // сделки происходит ТОЛЬКО через официальный резолв рынка.
    if (this.openPosition) {
      if (market.eventSlug === this.openPosition.market.eventSlug && price < this.openPosition.minPriceSinceEntry) {
        this.openPosition.minPriceSinceEntry = price;
      }
      return;
    }

    if (this.attemptInProgress) return;

    this.checkHourlyReset();

    if (this.tradesThisHour >= settings.quotaPerHour) return; // квота часа выполнена — ждём новый час

    // Коридор цены токена — общий для ВСЕХ профилей, проверяем один раз.
    if (price < settings.entryPrice || price > settings.maxEntryPrice) return;

    const secToClose = (market.closeTimeMs - Date.now()) / 1000;
    if (secToClose < 0) return;

    const feed = PRICE_FEEDS[market.coin];
    if (!feed) return; // на всякий случай — монета без фида просто игнорируется

    // Цена монеты на момент открытия окна — считаем один раз на рынок,
    // не зависит от профиля.
    const openTimeMs = market.closeTimeMs - market.windowMinutes * 60 * 1000;
    let openPrice = this.openPrices.get(market.eventSlug);
    if (openPrice === undefined) {
      const p = feed.getPriceAt(openTimeMs);
      if (p === null) return; // фид ещё не накопил данные на момент открытия окна — пропускаем
      openPrice = p;
      this.openPrices.set(market.eventSlug, openPrice);
    }
    const coinNow = feed.getLatestPrice();
    if (coinNow === null) return;
    const pctMove = (coinNow - openPrice) / openPrice;

    // Проверяем профили по очереди — первый, который полностью совпал
    // (окно + порог движения в сторону токена), даёт сигнал на вход.
    for (const profile of settings.profiles) {
      if (secToClose > profile.entryWindowSec) continue; // этот профиль ещё не "открылся" по времени

      const pctThreshold = profile.pctMoveThresholds[market.coin];
      if (pctThreshold === undefined) continue; // монета без настроенного порога в этом профиле — пропускаем

      const passesMove = side === "Up" ? pctMove >= pctThreshold : pctMove <= -pctThreshold;
      if (!passesMove) continue;

      this.attemptInProgress = true;
      const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
      this.executeMarketEntry(market, side, tokenId, price, secToClose, pctMove, profile);
      return; // сигнал найден — дальше профили не проверяем
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

      return { orderId: resp.orderId ?? null, filledSize, avgPrice };
    } catch (err) {
      console.log(`   ⏳ Рыночный ордер (${params.side}) не исполнился: ${(err as Error).message}`);
      return { orderId: null, filledSize: 0, avgPrice: 0 };
    }
  }

  /**
   * НЕ блокирует торговую логику. Запускается "в фоне" сразу после
   * успешной покупки и ждёт финальный статус сделки через User Stream.
   * Если сделка в итоге провалилась ончейн (FAILED) — шлёт тревожный
   * алерт, чтобы можно было проверить баланс/позицию вручную.
   */
  private async verifyTradeConfirmation(orderId: string, market: CryptoUpDownMarket, side: "Up" | "Down"): Promise<void> {
    const confirmation = await userStream.waitForConfirmation(orderId, TRADE_CONFIRMATION_TIMEOUT_MS);

    if (confirmation === null) {
      console.log(
        `   ⚠️ Не дождались подтверждения сделки ончейн за ${TRADE_CONFIRMATION_TIMEOUT_MS / 1000}с (orderId: ${orderId}). ` +
          `CLOB репортил успех при покупке — статус ончейн просто неизвестен, это не обязательно проблема.`,
      );
      return;
    }

    if (confirmation.status === "FAILED") {
      console.log(`   🚨 КРИТИЧНО: сделка НЕ подтвердилась ончейн (FAILED), хотя CLOB репортил success! orderId: ${orderId}`);
      if (this.telegram) {
        await this.telegram.send(
          `🚨 ВНИМАНИЕ: покупка "${market.title}" (${side}) была отклонена в сети (FAILED) уже ПОСЛЕ того как CLOB сказал, что всё прошло успешно.\nПроверь баланс и позицию вручную — возможно, деньги не списались/не купилось на самом деле, но бот думает, что позиция открыта.`,
        );
      }
      return;
    }

    console.log(`   ✅ Сделка подтверждена ончейн: ${confirmation.status} (orderId: ${orderId})`);
  }

  private async executeMarketEntry(
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    tokenId: string,
    priceAtEntry: number,
    secToClose: number,
    pctMove: number,
    profile: EntryProfile,
  ): Promise<void> {
    const size = TRADE_SIZE_USD / settings.entryPrice;
    const thresholdUsed = profile.pctMoveThresholds[market.coin] ?? 0;

    console.log(
      `\n⚡ ВХОД ПО РЫНКУ [${profile.name}]: [${market.coin} / 5мин] "${market.title}"\n` +
        `   Сторона: ${side} | Цена сейчас: ~${priceAtEntry} (коридор ${settings.entryPrice}-${settings.maxEntryPrice}) | До закрытия: ${secToClose.toFixed(1)}с (окно профиля: ${profile.entryWindowSec}с)\n` +
        `   Движение ${market.coin} от открытия окна: ${(pctMove * 100).toFixed(3)}% (порог профиля: ${(thresholdUsed * 100).toFixed(2)}%)\n` +
        `   Покупаем: ${size.toFixed(2)} акций рыночным ордером (~$${TRADE_SIZE_USD}) — держим до резолва`,
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
    };
    this.openPosition = pos;

    if (this.telegram) {
      await this.telegram.send(
        `💰 Куплено по рынку [${profile.name}]: ${market.title}\nСторона: ${side}\nЦена: ${result.avgPrice.toFixed(4)} | Размер: ${result.filledSize.toFixed(2)}\nДвижение ${market.coin} от открытия окна: ${(pctMove * 100).toFixed(3)}% (порог профиля: ${(thresholdUsed * 100).toFixed(2)}%)\nДержим до официального резолва (сделка ${this.tradesThisHour + 1}/${settings.quotaPerHour} в этом часе).`,
      );
    }

    // Параллельно (не блокируя) проверяем финальное ончейн-подтверждение.
    if (!DRY_RUN && result.orderId) {
      this.verifyTradeConfirmation(result.orderId, market, side).catch((err) =>
        console.error("[userStream] ошибка проверки подтверждения:", (err as Error).message),
      );
    }

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
      await this.notifyClose(pos, outcome, profit);
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
      `✅ Сделка завершена (всего с запуска: ${this.tradesTotal}, в этом часе: ${this.tradesThisHour}/${settings.quotaPerHour}). ` +
        `${this.tradesThisHour >= settings.quotaPerHour ? "Квота часа выполнена — жду следующий час." : "Возвращаюсь к мониторингу."}`,
    );

    this.refreshMarkets().catch((err) => console.error("[refresh] ошибка:", (err as Error).message));
  }

  private async notifyClose(pos: OpenPosition, outcome: "WIN" | "LOSS", profit: number): Promise<void> {
    const sign = outcome === "WIN" ? "✅ ПРИБЫЛЬ" : "🔻 УБЫТОК";

    const drawdown = Math.max(0, pos.buyPrice - pos.minPriceSinceEntry);
    const drawdownPct = pos.buyPrice > 0 ? (drawdown / pos.buyPrice) * 100 : 0;

    const msg =
      `${sign} (${outcome})\n` +
      `Способ закрытия: резолв рынка\n` +
      `${pos.market.title}\n` +
      `Сторона: ${pos.side}\n` +
      `Профит: ${profit >= 0 ? "+" : ""}$${profit.toFixed(3)}\n` +
      `Цена покупки: ${pos.buyPrice.toFixed(4)} | Мин. цена за сделку: ${pos.minPriceSinceEntry.toFixed(4)}\n` +
      `Максимальная просадка: ${drawdown.toFixed(3)} (${drawdownPct.toFixed(1)}%)`;

    console.log(`\n${msg}\n`);
    if (this.telegram) await this.telegram.send(msg);
  }

  start(): void {
    this.checkHourlyReset();
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

const COIN_ALIASES: Record<string, string> = {
  btc: "Bitcoin", бтс: "Bitcoin", биткоин: "Bitcoin", bitcoin: "Bitcoin",
  eth: "Ethereum", эфир: "Ethereum", ethereum: "Ethereum",
  sol: "Solana", солана: "Solana", solana: "Solana",
  xrp: "XRP", рипл: "XRP",
  doge: "Dogecoin", дож: "Dogecoin", доге: "Dogecoin", dogecoin: "Dogecoin",
};

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
          await telegram?.send(`Нижняя цена входа установлена (общая на все профили): ${settings.entryPrice}`);
          continue;
        }

        const maxPriceMatch = text.match(/^цена_макс\s+([\d.]+)$/);
        if (maxPriceMatch) {
          settings.maxEntryPrice = Number(maxPriceMatch[1]);
          await telegram?.send(`Верхняя цена входа установлена (общая на все профили): ${settings.maxEntryPrice}`);
          continue;
        }

        const quotaMatch = text.match(/^квота\s+(\d+)$/);
        if (quotaMatch) {
          settings.quotaPerHour = Math.max(1, Number(quotaMatch[1]));
          await telegram?.send(`Квота установлена: ${settings.quotaPerHour} сделок в час.`);
          continue;
        }

        // ─── Команды, адресованные КОНКРЕТНОМУ профилю: "профиль N ..." ───
        const profileWindowMatch = text.match(/^профиль\s+(\d+)\s+окно\s+(\d+)$/);
        if (profileWindowMatch) {
          const idx = Number(profileWindowMatch[1]) - 1;
          const profile = settings.profiles[idx];
          if (!profile) {
            await telegram?.send(`Нет профиля №${profileWindowMatch[1]}. Всего профилей: ${settings.profiles.length}.`);
            continue;
          }
          profile.entryWindowSec = Number(profileWindowMatch[2]);
          await telegram?.send(`[${profile.name}] окно входа установлено: последние ${profile.entryWindowSec}с перед закрытием`);
          continue;
        }

        const profileMoveCoinMatch = text.match(/^профиль\s+(\d+)\s+движение\s+(\S+)\s+([\d.]+)$/);
        if (profileMoveCoinMatch) {
          const idx = Number(profileMoveCoinMatch[1]) - 1;
          const profile = settings.profiles[idx];
          if (!profile) {
            await telegram?.send(`Нет профиля №${profileMoveCoinMatch[1]}. Всего профилей: ${settings.profiles.length}.`);
            continue;
          }
          const alias = profileMoveCoinMatch[2].toLowerCase();
          const coin = COIN_ALIASES[alias];
          if (!coin) {
            await telegram?.send(`Не узнал монету "${profileMoveCoinMatch[2]}". Варианты: btc, eth, sol, xrp, doge.`);
            continue;
          }
          profile.pctMoveThresholds[coin] = Number(profileMoveCoinMatch[3]);
          await telegram?.send(
            `[${profile.name}] порог движения для ${coin} установлен: ${(profile.pctMoveThresholds[coin] * 100).toFixed(2)}%`,
          );
          continue;
        }

        const profileMoveAllMatch = text.match(/^профиль\s+(\d+)\s+движение\s+([\d.]+)$/);
        if (profileMoveAllMatch) {
          const idx = Number(profileMoveAllMatch[1]) - 1;
          const profile = settings.profiles[idx];
          if (!profile) {
            await telegram?.send(`Нет профиля №${profileMoveAllMatch[1]}. Всего профилей: ${settings.profiles.length}.`);
            continue;
          }
          const value = Number(profileMoveAllMatch[2]);
          for (const c of COINS) profile.pctMoveThresholds[c] = value;
          await telegram?.send(`[${profile.name}] порог движения установлен для всех монет: ${(value * 100).toFixed(2)}%`);
          continue;
        }

        // ─── Старые команды без "профиль N" — обратная совместимость,
        // адресуются ПРОФИЛЮ 1 (основному) ───
        const windowMatch = text.match(/^окно\s+(\d+)$/);
        if (windowMatch) {
          const profile = settings.profiles[0];
          profile.entryWindowSec = Number(windowMatch[1]);
          await telegram?.send(`[${profile.name}] окно входа установлено: последние ${profile.entryWindowSec}с перед закрытием`);
          continue;
        }

        const moveCoinMatch = text.match(/^движение\s+(\S+)\s+([\d.]+)$/);
        if (moveCoinMatch) {
          const profile = settings.profiles[0];
          const alias = moveCoinMatch[1].toLowerCase();
          const coin = COIN_ALIASES[alias];
          if (!coin) {
            await telegram?.send(`Не узнал монету "${moveCoinMatch[1]}". Варианты: btc, eth, sol, xrp, doge.`);
            continue;
          }
          profile.pctMoveThresholds[coin] = Number(moveCoinMatch[2]);
          await telegram?.send(
            `[${profile.name}] порог движения для ${coin} установлен: ${(profile.pctMoveThresholds[coin] * 100).toFixed(2)}%`,
          );
          continue;
        }

        const moveAllMatch = text.match(/^движение\s+([\d.]+)$/);
        if (moveAllMatch) {
          const profile = settings.profiles[0];
          const value = Number(moveAllMatch[1]);
          for (const c of COINS) profile.pctMoveThresholds[c] = value;
          await telegram?.send(`[${profile.name}] порог движения установлен для всех монет: ${(value * 100).toFixed(2)}%`);
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
  console.log("=== FASTFLIP BUILD: v7.0 — НЕСКОЛЬКО ПРОФИЛЕЙ ВХОДА ОДНОВРЕМЕННО ===");
  console.log(`Режим: ${DRY_RUN ? "DRY_RUN (без реальных сделок, полная симуляция)" : "⚠️  LIVE — РЕАЛЬНЫЕ ДЕНЬГИ"}`);
  console.log(`Коридор входа (общий на все профили): ${settings.entryPrice}-${settings.maxEntryPrice} | Квота: ${settings.quotaPerHour}/час`);
  for (const [i, p] of settings.profiles.entries()) {
    console.log(
      `  Профиль ${i + 1} "${p.name}": окно ${p.entryWindowSec}с, пороги: ` +
        COINS.map((c) => `${c} ${((p.pctMoveThresholds[c] ?? 0) * 100).toFixed(2)}%`).join(", "),
    );
  }

  // запускаем все фиды цены (Polymarket RTDS, Chainlink TWAP) до старта самого бота
  btcPriceFeed.start();
  ethPriceFeed.start();
  solPriceFeed.start();
  xrpPriceFeed.start();
  dogePriceFeed.start();

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

    userStream.start(clob.getApiCreds());
    console.log("User Stream запущен — сделки будут дополнительно проверяться на ончейн-подтверждение.");
  }

  const logger = createLogger(false);
  const telegram = createTelegramNotifier(process.env.TELEGRAM_BOT_TOKEN, process.env.TELEGRAM_CHAT_ID, logger);

  const bot = new FastFlipMarketBot(clob, telegram);
  bot.start();

  if (telegram && process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID) {
    console.log(
      "Telegram-команды включены: цена X / цена_макс X / квота X / статус / " +
        "профиль N окно X / профиль N движение <монета> X / профиль N движение X (всем монетам профиля) / " +
        "окно X и движение ... без номера профиля — адресуются профилю 1",
    );
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
