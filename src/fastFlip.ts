/**
 * "Быстрый флип" v6.2 — РЫНОЧНЫЕ ордера на ВХОД. НИКАКИХ ПРОДАЖ ДО
 * ОФИЦИАЛЬНОГО РЕЗОЛВА — ни тейка, ни стоп-лосса, ни аварийных выходов.
 * Купил → держишь до резолва рынка через Gamma API. Точка.
 *
 * v6.2 (относительно v6.0/v6.1): СТОП-ЛОСС ПОЛНОСТЬЮ УБРАН вместе с
 * emergencyExit/isBadFillLow. Причина: в реальных логах стоп срабатывал
 * от единичного аномального тика цены (например 0.97 → 0.03 за доли
 * секунды — комплементарная/шумовая цена), что не отражало реальный
 * рынок, и бот продавал позицию почти сразу в убыток. Раз риск ложных
 * срабатываний выше пользы от защиты — стоп убран целиком, без
 * возможности включить через settings/Telegram. Единственный путь
 * закрытия сделки теперь — resolveWinner() через официальный Gamma API.
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

const REDEEM_POLL_MS = 60 * 1000;
const GAMMA_HOST = "https://gamma-api.polymarket.com";

// ─── Настройки, которые можно менять на лету через Telegram ───
const settings = {
  entryPrice: Number(process.env.FASTFLIP_ENTRY_PRICE ?? "0.97"),
  maxEntryPrice: Number(process.env.FASTFLIP_MAX_ENTRY_PRICE ?? "0.98"),
  entryWindowSec: Number(process.env.FASTFLIP_ENTRY_WINDOW_SEC ?? "60"),
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

  constructor(
    private clob: ClobService | null,
    private telegram: ReturnType<typeof createTelegramNotifier>,
  ) {}

  getStatus(): string {
    const watchedMarkets = this.tokenIndex.size / 2;
    return (
      `Режим: рыночный вход, БЕЗ ДОСРОЧНЫХ ПРОДАЖ — держим ВСЕГДА до резолва, BTC\n` +
      `Вход: ${settings.entryPrice}–${settings.maxEntryPrice} | Окно входа: последние ${settings.entryWindowSec}с\n` +
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

    // Позиция уже открыта — v6.2: НИКАКОЙ реакции на цену вообще, кроме
    // отслеживания минимума (для статистики просадки в отчёте о
    // резолве). Никаких продаж на основании тика цены — только резолв.
    if (this.openPosition) {
      if (market.eventSlug === this.openPosition.market.eventSlug && price < this.openPosition.minPriceSinceEntry) {
        this.openPosition.minPriceSinceEntry = price;
      }
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
      closed: false,
      minPriceSinceEntry: result.avgPrice,
      resolveFallbackTimer: null,
    };
    this.openPosition = pos;

    // v6.2: НИКАКИХ проверок "плохого филла", никаких аварийных
    // продаж. Что бы ни случилось с ценой исполнения — держим до
    // официального резолва.

    if (this.telegram) {
      await this.telegram.send(
        `💰 Куплено по рынку: ${market.title}\nСторона: ${side}\nЦена: ${result.avgPrice.toFixed(4)} | Размер: ${result.filledSize.toFixed(2)}\nДержим ДО ОФИЦИАЛЬНОГО РЕЗОЛВА. Никаких досрочных продаж вообще.`,
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
  // МЕТКА ВЕРСИИ — если в логах при старте бота НЕТ этой строки,
  // значит запущен не этот файл (старая сборка / другой процесс).
  console.log("=== FASTFLIP BUILD: v6.2 — ТОЛЬКО ВХОД, ДЕРЖИМ ДО РЕЗОЛВА, СТОПА НЕТ ВООБЩЕ ===");
  console.log(`Режим: ${DRY_RUN ? "DRY_RUN (без реальных сделок, полная симуляция)" : "⚠️  LIVE — РЕАЛЬНЫЕ ДЕНЬГИ"}`);
  console.log(
    `Актив: BTC only | Вход: ${settings.entryPrice}-${settings.maxEntryPrice} (рынком, окно ${settings.entryWindowSec}с) | ` +
      `Выход: ТОЛЬКО официальный резолв. Тейка нет. Стоп-лосса нет. Аварийных продаж нет. | Квота: ${settings.quotaPerHour}/час`,
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
    console.log("Telegram-команды включены: цена X / цена_макс X / окно X / квота X / статус");
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