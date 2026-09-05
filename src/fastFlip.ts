/**
 * "Быстрый флип" v2 — торговый бот, ПАРАЛЛЕЛЬНЫЙ старому sniperTrader.ts
 * (тот не запускаем, код не трогаем).
 *
 * Логика:
 *  1. Торгуем ТОЛЬКО BTC, ТОЛЬКО 5-минутные Up/Down рынки.
 *  2. РОВНО 1 сделка в час: в начале каждого часа рандомно выбираем одну
 *     из 12 пятиминуток этого часа и следим только за ней. Остальные
 *     11 пятиминуток в этом часе полностью игнорируются.
 *  3. Как цена выбранного рынка (Up или Down) впервые касается ENTRY_PRICE
 *     (0.98) — покупаем.
 *  4. Как покупка исполнилась — сразу пытаемся выставить лимитку на
 *     продажу по TP_PRICE (0.999). Если выставление не удалось — ПРОДОЛЖАЕМ
 *     пытаться выставить её же, вплоть до закрытия окна.
 *  5. Параллельно следим за живой ценой. Если цена падает до STOP_PRICE
 *     (0.60 по умолчанию) — отменяем тейк-лимитку и ПЫТАЕМСЯ продать
 *     по рынку (FAK), повторяя попытки, пока не продастся.
 *  6. Если ни тейк, ни стоп не сработали до закрытия — ждём официального
 *     резолва через Gamma API (с ретраями) и всё равно шлём итог.
 *  7. Каждое закрытие сделки (тейк / стоп / резолв) — сообщение в Telegram
 *     с чётким WIN/LOSS.
 *
 * ВАЖНО: во всех трёх путях закрытия (тейк / стоп / резолв) обязательно
 * обнуляем this.openPosition — иначе refreshMarkets() навсегда думает,
 * что позиция ещё открыта, и никогда не начинает искать сделку в
 * следующих часах (это был баг: после закрытия через резолв openPosition
 * не сбрасывался, и бот замолкал навсегда после первой же такой сделки).
 *
 * НАСТРОЙКИ МЕНЯЮТСЯ ЧЕРЕЗ TELEGRAM НА ЛЕТУ (без передеплоя):
 *   цена 0.98    — цена входа
 *   тейк 0.999   — цена тейк-профита
 *   стоп 0.6     — цена стоп-лосса
 *   статус       — текущие настройки + что происходит сейчас
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
const HOUR_MS = 60 * 60 * 1000;
const SLOT_MS = 5 * 60 * 1000;
const SLOTS_PER_HOUR = 12;

const MARKET_REFRESH_MS = 15 * 1000;
const OBSERVE_WINDOW_MS = 65 * 60 * 1000; // видим цель на весь час вперёд
const SLOT_MATCH_TOLERANCE_MS = 60 * 1000;

const FILL_CHECK_INTERVAL_MS = 5 * 1000;
const ORDER_RETRY_DELAY_MS = 3 * 1000;
const STOP_SELL_RETRY_DELAY_MS = 3 * 1000;
const STOP_SELL_GIVEUP_AFTER_MS = 90 * 1000; // после закрытия окна ещё пытаемся столько

const RESOLVE_CHECK_DELAY_SEC = 180;
const RESOLVE_RETRY_MS = 30 * 1000;
const RESOLVE_GIVE_UP_MS = 30 * 60 * 1000;

const REDEEM_POLL_MS = 60 * 1000;
const GAMMA_HOST = "https://gamma-api.polymarket.com";

// ─── Настройки, которые можно менять на лету через Telegram ───
const settings = {
  entryPrice: Number(process.env.FASTFLIP_ENTRY_PRICE ?? "0.98"),
  tpPrice: Number(process.env.FASTFLIP_TP_PRICE ?? "0.999"),
  stopPrice: Number(process.env.FASTFLIP_STOP_PRICE ?? "0.60"),
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
  tpOrderId: string | null;
  stopTriggered: boolean;
  closed: boolean;
}

interface HourlyTarget {
  slotStartMs: number;
  slotCloseMs: number;
}

function buildTokenIndex(markets: CryptoUpDownMarket[]): Map<string, TokenInfo> {
  const idx = new Map<string, TokenInfo>();
  for (const m of markets) {
    idx.set(m.upTokenId, { market: m, side: "Up" });
    idx.set(m.downTokenId, { market: m, side: "Down" });
  }
  return idx;
}

function pickHourlyTarget(): HourlyTarget | null {
  const now = Date.now();
  const hourStart = Math.floor(now / HOUR_MS) * HOUR_MS;
  const candidates: HourlyTarget[] = [];
  for (let k = 0; k < SLOTS_PER_HOUR; k++) {
    const slotStartMs = hourStart + k * SLOT_MS;
    const slotCloseMs = slotStartMs + SLOT_MS;
    // Не берём пятиминутку, которая уже закрылась или закрывается прямо сейчас
    if (slotCloseMs > now + 5000) candidates.push({ slotStartMs, slotCloseMs });
  }
  if (candidates.length === 0) return null;
  return candidates[Math.floor(Math.random() * candidates.length)];
}

/** Официальный резолв рынка через Gamma API (для редкого fallback-кейса). */
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

class FastFlipBot {
  private watcher: PriceWatcher | null = null;
  private tokenIndex = new Map<string, TokenInfo>();
  private lastTokenIds: string[] = [];

  private currentHourKey: number | null = null;
  private currentTarget: HourlyTarget | null = null;
  private hasTradedThisHour = false;

  private openPosition: OpenPosition | null = null;
  private updateCount = 0;

  constructor(
    private clob: ClobService | null,
    private telegram: ReturnType<typeof createTelegramNotifier>,
  ) {}

  getStatus(): string {
    const target = this.currentTarget
      ? `${new Date(this.currentTarget.slotStartMs).toISOString()} → ${new Date(this.currentTarget.slotCloseMs).toISOString()}`
      : "не выбрана";
    return (
      `Цена входа: ${settings.entryPrice} | Тейк: ${settings.tpPrice} | Стоп: ${settings.stopPrice}\n` +
      `Цель этого часа: ${target}\n` +
      `Сделка в этом часе: ${this.hasTradedThisHour ? "уже была" : "ещё нет"}\n` +
      `Открытая позиция: ${this.openPosition ? `${this.openPosition.market.title} (${this.openPosition.side})` : "нет"}`
    );
  }

  private maybeReselectTarget(): void {
    const now = Date.now();
    const hourKey = Math.floor(now / HOUR_MS);
    if (hourKey !== this.currentHourKey) {
      this.currentHourKey = hourKey;
      this.hasTradedThisHour = false;
      this.currentTarget = pickHourlyTarget();
      const msg = this.currentTarget
        ? `🎲 Новый час. Цель: пятиминутка ${new Date(this.currentTarget.slotStartMs).toISOString()} - ${new Date(this.currentTarget.slotCloseMs).toISOString()}`
        : `🎲 Новый час, но не удалось выбрать пятиминутку.`;
      console.log(msg);
    }
  }

  async refreshMarkets(): Promise<void> {
    this.maybeReselectTarget();

    if (this.hasTradedThisHour || !this.currentTarget || this.openPosition) {
      if (!this.openPosition) {
        this.watcher?.stop();
        this.watcher = null;
        this.tokenIndex = new Map();
        this.lastTokenIds = [];
      }
      return;
    }

    let allMarkets: CryptoUpDownMarket[];
    try {
      allMarkets = await discoverCryptoUpDownMarkets([{ suffixes: ["up-or-down-5m"], minutes: 5 }]);
    } catch (err) {
      console.error("[refresh] ошибка:", (err as Error).message);
      return;
    }

    const now = Date.now();
    const target = this.currentTarget;
    const markets = allMarkets.filter(
      (m) =>
        m.coin === COIN &&
        m.closeTimeMs - now <= OBSERVE_WINDOW_MS &&
        Math.abs(m.closeTimeMs - target.slotCloseMs) <= SLOT_MATCH_TOLERANCE_MS,
    );

    this.tokenIndex = buildTokenIndex(markets);
    const tokenIds = [...this.tokenIndex.keys()].sort();

    console.log(
      `[refresh] цель часа: рынков найдено ${markets.length} (${tokenIds.length} токенов)`,
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

    // ── Если есть открытая позиция — проверяем стоп-лосс ──
    const pos = this.openPosition;
    if (pos && update.tokenId === pos.tokenId && !pos.closed && !pos.stopTriggered) {
      const price = update.bestBid ?? update.bestAsk;
      if (price !== null && price <= settings.stopPrice) {
        pos.stopTriggered = true;
        this.triggerStopLoss(pos).catch((err) =>
          console.error("[stop] необработанная ошибка:", (err as Error).message),
        );
      }
      return;
    }
    if (pos) return; // позиция открыта — не ищем новых входов

    // ── Поиск входа ──
    const info = this.tokenIndex.get(update.tokenId);
    if (!info) return;
    const { market, side } = info;

    const price = update.bestBid ?? update.bestAsk;
    if (price === null || price < settings.entryPrice) return;

    if (this.hasTradedThisHour) return;
    this.hasTradedThisHour = true;

    const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
    this.executeFlip(market, side, tokenId, price);
  }

  private async executeFlip(
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    tokenId: string,
    priceAtEntry: number,
  ): Promise<void> {
    const size = TRADE_SIZE_USD / settings.entryPrice;

    console.log(
      `\n⚡ ВХОД: [BTC / 5мин] "${market.title}"\n` +
        `   Сторона: ${side} | Цена сейчас: ~${priceAtEntry} | Покупаем: ${size.toFixed(2)} акций по ${settings.entryPrice} (~$${TRADE_SIZE_USD})`,
    );

    if (DRY_RUN || !this.clob) {
      console.log(`   [DRY RUN] Ордер НЕ отправлен. eventSlug: ${market.eventSlug}`);
      if (this.telegram) {
        await this.telegram.send(
          `⚡ [DRY RUN] Вход: ${market.title}\nСторона: ${side}\nЦена: ${settings.entryPrice}`,
        );
      }
      return;
    }

    const buyDeadline = market.closeTimeMs + 30 * 1000;
    let buyOrderId: string | null = null;

    while (Date.now() < buyDeadline && !buyOrderId) {
      try {
        const result = await this.clob.placeGtcLimitOrder({
          tokenId,
          side: Side.BUY,
          price: settings.entryPrice,
          size,
          offsetPct: 0,
        });
        if (result.orderId) {
          buyOrderId = result.orderId;
          console.log(`   ✅ ЗАЯВКА НА ПОКУПКУ: orderId=${buyOrderId} status=${result.status}`);
        }
      } catch (err) {
        console.error(`   ❌ ОШИБКА ПОКУПКИ, повторяем:`, (err as Error).message);
      }
      if (!buyOrderId) await new Promise((r) => setTimeout(r, ORDER_RETRY_DELAY_MS));
    }

    if (!buyOrderId) {
      console.log(`   ⏳ Не удалось выставить покупку до закрытия (eventSlug: ${market.eventSlug}).`);
      return;
    }

    this.managePosition(market, side, tokenId, buyOrderId, settings.entryPrice);
  }

  private async managePosition(
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    tokenId: string,
    buyOrderId: string,
    buyPrice: number,
  ): Promise<void> {
    if (!this.clob) return;

    // Шаг 1: ждём исполнения покупки
    const buyFillDeadline = market.closeTimeMs + 30 * 1000;
    let filledSize = 0;
    while (Date.now() < buyFillDeadline) {
      await new Promise((r) => setTimeout(r, FILL_CHECK_INTERVAL_MS));
      try {
        const order = await this.clob.getOrder(buyOrderId);
        const matched = Number(
          (order as any)?.size_matched ?? (order as any)?.sizeMatched ?? (order as any)?.filledSize ?? 0,
        );
        if (matched > 0) {
          filledSize = matched;
          break;
        }
      } catch (err) {
        console.error(`   [ожидание покупки] ошибка:`, (err as Error).message);
      }
    }

    if (filledSize <= 0) {
      console.log(`   ⏳ Покупка не исполнилась (eventSlug: ${market.eventSlug}).`);
      return;
    }

    console.log(`   💰 ПОКУПКА ИСПОЛНЕНА: ${filledSize.toFixed(2)} акций.`);

    const pos: OpenPosition = {
      market,
      side,
      tokenId,
      buyPrice,
      filledSize,
      tpOrderId: null,
      stopTriggered: false,
      closed: false,
    };
    this.openPosition = pos;

    if (this.telegram) {
      await this.telegram.send(
        `💰 Куплено: ${market.title}\nСторона: ${side}\nЦена: ${buyPrice} | Размер: ${filledSize.toFixed(2)}\nСтавлю тейк ${settings.tpPrice} и слежу за стопом ${settings.stopPrice}...`,
      );
    }

    // Шаг 2: пытаемся выставить тейк-профит, повторяем пока не получится
    // или сделка не закроется (по стопу) или не закроется окно.
    const tpDeadline = market.closeTimeMs + 30 * 1000;
    while (Date.now() < tpDeadline && !pos.tpOrderId && !pos.closed) {
      try {
        const tpResult = await this.clob.placeGtcLimitOrder({
          tokenId,
          side: Side.SELL,
          price: settings.tpPrice,
          size: filledSize,
          offsetPct: 0,
        });
        if (tpResult.orderId) {
          pos.tpOrderId = tpResult.orderId;
          console.log(`   ✅ ТЕЙК-ПРОФИТ ВЫСТАВЛЕН: orderId=${pos.tpOrderId}`);
        }
      } catch (err) {
        console.error(`   ❌ ОШИБКА ТЕЙК-ПРОФИТА, повторяем:`, (err as Error).message);
      }
      if (!pos.tpOrderId && !pos.closed) await new Promise((r) => setTimeout(r, ORDER_RETRY_DELAY_MS));
    }

    // Шаг 3: ждём исполнения тейка до закрытия рынка (стоп обрабатывается
    // асинхронно в onPriceUpdate/triggerStopLoss параллельно с этим циклом).
    if (pos.tpOrderId) {
      const fillDeadline = market.closeTimeMs + 30 * 1000;
      while (Date.now() < fillDeadline && !pos.closed) {
        await new Promise((r) => setTimeout(r, FILL_CHECK_INTERVAL_MS));
        if (pos.closed) break;
        try {
          const order = await this.clob.getOrder(pos.tpOrderId);
          const matched = Number(
            (order as any)?.size_matched ?? (order as any)?.sizeMatched ?? (order as any)?.filledSize ?? 0,
          );
          if (matched >= filledSize - 0.001) {
            pos.closed = true;
            const profit = matched * (settings.tpPrice - buyPrice);
            await this.notifyClose(market, side, "тейк-профит", "WIN", profit);
            if (this.openPosition === pos) this.openPosition = null;
            return;
          }
        } catch (err) {
          console.error(`   [ожидание тейка] ошибка:`, (err as Error).message);
        }
      }
    }

    if (pos.closed) {
      if (this.openPosition === pos) this.openPosition = null;
      return;
    }

    console.log(`   ⏳ Ни тейк, ни стоп не сработали до закрытия — жду официальный резолв (eventSlug: ${market.eventSlug}).`);
    this.scheduleResolveFallback(pos);
  }

  private async triggerStopLoss(pos: OpenPosition): Promise<void> {
    if (!this.clob) return;
    console.log(`   🛑 СТОП-ЛОСС СРАБОТАЛ (цена ≤ ${settings.stopPrice}): ${pos.market.title}`);

    if (pos.tpOrderId) {
      try {
        await this.clob.cancelOrders([pos.tpOrderId]);
        console.log(`   Тейк-лимитка отменена.`);
      } catch (err) {
        console.error(`   ⚠️ Не удалось отменить тейк-лимитку:`, (err as Error).message);
      }
    }

    const giveUpAt = pos.market.closeTimeMs + STOP_SELL_GIVEUP_AFTER_MS;
    while (Date.now() < giveUpAt && !pos.closed) {
      try {
        const result = await this.clob.placeLimitOrder({
          tokenId: pos.tokenId,
          side: Side.SELL,
          price: settings.stopPrice,
          size: pos.filledSize,
        });
        const soldSize = Number(result.filledSize ?? pos.filledSize);
        const soldUsdc = Number(result.filledUsdc ?? soldSize * settings.stopPrice);
        const avgExitPrice = soldSize > 0 ? soldUsdc / soldSize : settings.stopPrice;
        const profit = soldSize * avgExitPrice - soldSize * pos.buyPrice;

        pos.closed = true;
        await this.notifyClose(pos.market, pos.side, "стоп-лосс", "LOSS", profit);
        console.log(`   ✅ Продано по стопу: exit≈${avgExitPrice.toFixed(3)} профит=$${profit.toFixed(3)}`);
        if (this.openPosition === pos) this.openPosition = null;
        return;
      } catch (err) {
        console.error(`   ❌ ОШИБКА ПРОДАЖИ ПО СТОПУ, повторяем:`, (err as Error).message);
        await new Promise((r) => setTimeout(r, STOP_SELL_RETRY_DELAY_MS));
      }
    }

    if (!pos.closed) {
      console.log(`   ⚠️ Не удалось продать по стопу до дедлайна — оставляем висеть, дождёмся резолва.`);
      this.scheduleResolveFallback(pos);
    }
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
          if (this.openPosition === pos) this.openPosition = null;
          return;
        }
        setTimeout(check, RESOLVE_RETRY_MS);
        return;
      }
      pos.closed = true;
      const outcome: "WIN" | "LOSS" = winner === pos.side ? "WIN" : "LOSS";
      const profit =
        outcome === "WIN"
          ? pos.filledSize * (1 - pos.buyPrice)
          : -pos.filledSize * pos.buyPrice;
      await this.notifyClose(pos.market, pos.side, "резолв рынка", outcome, profit);
      if (this.openPosition === pos) this.openPosition = null;
    };
    setTimeout(check, RESOLVE_RETRY_MS);
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
      console.log(`--- статус: апдейтов ${this.updateCount}, сделка в этом часе: ${this.hasTradedThisHour} ---`);
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
  bot: FastFlipBot,
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

        const stopMatch = text.match(/^стоп\s+([\d.]+)$/);
        if (stopMatch) {
          settings.stopPrice = Number(stopMatch[1]);
          await telegram?.send(`Стоп-лосс установлен: ${settings.stopPrice}`);
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
  console.log(`Режим: ${DRY_RUN ? "DRY_RUN (без реальных сделок)" : "⚠️  LIVE — РЕАЛЬНЫЕ ДЕНЬГИ"}`);
  console.log(`Актив: BTC only | 1 сделка/час | Вход: ${settings.entryPrice} | Тейк: ${settings.tpPrice} | Стоп: ${settings.stopPrice}`);

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

  const bot = new FastFlipBot(clob, telegram);
  bot.start();

  if (telegram && process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID) {
    console.log("Telegram-команды включены: цена X / тейк X / стоп X / статус");
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