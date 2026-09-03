/**
 * "Быстрый флип" — отдельная, ПАРАЛЛЕЛЬНАЯ стратегия, не связана с
 * sniperTrader.ts (тот пока просто не запускаем, код не трогаем).
 * "Быстрый флип" — торговый бот, ПАРАЛЛЕЛЬНЫЙ старому sniperTrader.ts
 * (тот не запускаем, код не трогаем).
 *
 * Только Bitcoin, только 5-минутные рынки.
 * Логика на рынок:
 *  1. Следим за ценой с самого начала каждого 5-мин окна.
 *  2. Как только цена любой стороны впервые достигает ENTRY_PRICE —
 *     это "подходящий момент". Применяем селект (чередование через
 *     один) и дневной лимит. Если прошли — ставим GTC-лимитку на
 *     ПОКУПКУ по ENTRY_PRICE.
 *  3. Как только покупка исполнилась — сразу ставим GTC-лимитку на
 *     ПРОДАЖУ (тейк-профит) по 0.999, чтобы зафиксировать прибыль до
 *     резолва, если получится.
 *  4. Если тейк-профит не исполнился до закрытия — ждём официальный
 *     резолв (через Gamma API, как в researchLogger.ts), узнаём
 *     реальный исход и шлём в Telegram итог сделки. Авто-клейм заберёт
 *     деньги отдельно, эта проверка нужна только для уведомления.
 *
 * Логика:
 *  1. Следим за ценой с самого начала каждого 5-мин окна (не ждём
 *     конца окна).
 *  2. Как только цена любой стороны (Up/Down) впервые достигает 0.99 —
 *     сразу ставим GTC-лимитку на ПОКУПКУ этой стороны по цене 0.99
 *     (лимитка, не рыночный ордер — чтобы не проскользить выше 0.99
 *     на неглубоком стакане и не съесть маржу).
 *  3. Ждём исполнения этой заявки (проверяем статус каждые несколько
 *     секунд).
 *  4. Как только заявка исполнилась (хотя бы частично) — сразу ставим
 *     GTC-лимитку на ПРОДАЖУ исполненного объёма по цене 0.999
 *     (тейк-профит, чтобы зафиксировать прибыль СРАЗУ, не дожидаясь
 *     официального резолва рынка).
 *  5. Если тейк-профит не исполнился до резолва — просто оставляем
 *     висеть. Авто-клейм (тот же модуль, что и в sniperTrader.ts) всё
 *     равно заберёт выигрыш после официального резолва.
 * НАСТРОЙКИ МЕНЯЮТСЯ ЧЕРЕЗ TELEGRAM НА ЛЕТУ (без передеплоя):
 *   лимит 25        — дневной лимит сделок
 *   лимит нет       — снять лимит (без ограничения)
 *   цена 0.98       — цена входа
 *   монеты BTC,SOL,ETH,DOGE  — список монет для торговли
 *   статус          — текущие настройки + сколько сделок сегодня
 *
 * DRY_RUN=true по умолчанию — только логи, без реальных денег, пока не
 * выставишь FASTFLIP_DRY_RUN=false явно.
 * DRY_RUN=true по умолчанию (FASTFLIP_DRY_RUN=false для реальных денег).
 */

import "dotenv/config";
@@ -32,19 +33,41 @@ import { PriceWatcher, PriceUpdate } from "./priceWatcher.js";
import { ClobService } from "./clob.js";
import { RedeemService } from "./redeem.js";
import { DataApiClient } from "./dataApi.js";
import { createTelegramNotifier } from "./telegram.js";
import { createLogger } from "./logger.js";

const DRY_RUN = (process.env.FASTFLIP_DRY_RUN ?? "true").toLowerCase() !== "false";
const AUTO_REDEEM = (process.env.FASTFLIP_AUTO_REDEEM ?? "true").toLowerCase() !== "false";
const TRADE_SIZE_USD = Number(process.env.FASTFLIP_TRADE_SIZE_USD ?? "5");
const BUY_PRICE = Number(process.env.FASTFLIP_BUY_PRICE ?? "0.99");
const TP_PRICE = Number(process.env.FASTFLIP_TP_PRICE ?? "0.999");
const MARKET_REFRESH_MS = 30 * 1000;
const OBSERVE_WINDOW_MS = 6 * 60 * 1000; // весь 5-мин рынок с запасом
const OBSERVE_WINDOW_MS = 6 * 60 * 1000;
const FILL_CHECK_INTERVAL_MS = 5 * 1000;
const REDEEM_POLL_MS = 60 * 1000;

const TARGET_COIN = "Bitcoin";
const RESOLVE_CHECK_DELAY_SEC = 180;
const GAMMA_HOST = "https://gamma-api.polymarket.com";

const TICKER_TO_COIN: Record<string, string> = {
  BTC: "Bitcoin",
  ETH: "Ethereum",
  SOL: "Solana",
  XRP: "XRP",
  DOGE: "Dogecoin",
};
const COIN_TO_TICKER: Record<string, string> = Object.fromEntries(
  Object.entries(TICKER_TO_COIN).map(([t, c]) => [c, t]),
);

// ─── Настройки, которые можно менять на лету через Telegram ───
const settings = {
  dailyLimit: Number(process.env.FASTFLIP_DAILY_LIMIT ?? "25") as number | null,
  entryPrice: Number(process.env.FASTFLIP_ENTRY_PRICE ?? "0.98"),
  coins: (process.env.FASTFLIP_COINS ?? "BTC,SOL,ETH,DOGE")
    .split(",")
    .map((s) => s.trim().toUpperCase())
    .filter((t) => TICKER_TO_COIN[t])
    .map((t) => TICKER_TO_COIN[t]),
};

interface TokenInfo {
  market: CryptoUpDownMarket;
@@ -60,14 +83,33 @@ function buildTokenIndex(markets: CryptoUpDownMarket[]): Map<string, TokenInfo>
  return idx;
}

function todayKey(): string {
  return new Date().toISOString().slice(0, 10); // YYYY-MM-DD по UTC
}

class FastFlipBot {
  private watcher: PriceWatcher | null = null;
  private tokenIndex = new Map<string, TokenInfo>();
  private lastTokenIds: string[] = [];
  private entered = new Set<string>(); // eventSlug — уже вошли в этот рынок
  private entered = new Set<string>();
  private updateCount = 0;

  constructor(private clob: ClobService | null) {}
  private tradesToday = 0;
  private tradesTodayKey = todayKey();
  private skipNext = false; // селект: чередование через один

  constructor(
    private clob: ClobService | null,
    private telegram: ReturnType<typeof createTelegramNotifier>,
  ) {}

  getStatus(): string {
    return (
      `Лимит: ${settings.dailyLimit ?? "нет"} | Сделок сегодня: ${this.tradesToday}\n` +
      `Цена входа: ${settings.entryPrice}\n` +
      `Монеты: ${settings.coins.map((c) => COIN_TO_TICKER[c] ?? c).join(", ")}`
    );
  }

  async refreshMarkets(): Promise<void> {
    let allMarkets: CryptoUpDownMarket[];
@@ -80,14 +122,14 @@ class FastFlipBot {

    const now = Date.now();
    const markets = allMarkets.filter(
      (m) => m.coin === TARGET_COIN && m.closeTimeMs - now <= OBSERVE_WINDOW_MS,
      (m) => settings.coins.includes(m.coin) && m.closeTimeMs - now <= OBSERVE_WINDOW_MS,
    );

    this.tokenIndex = buildTokenIndex(markets);
    const tokenIds = [...this.tokenIndex.keys()].sort();

    console.log(
      `[refresh] Bitcoin 5-мин рынков: ${markets.length} (${tokenIds.length} токенов), входов сделано: ${this.entered.size}`,
      `[refresh] рынков: ${markets.length} (${tokenIds.length} токенов), сделок сегодня: ${this.tradesToday}/${settings.dailyLimit ?? "∞"}`,
    );

    const sameAsLastTime =
@@ -114,9 +156,31 @@ class FastFlipBot {
    if (this.entered.has(market.eventSlug)) return;

    const price = update.bestBid ?? update.bestAsk;
    if (price === null || price < BUY_PRICE) return;
    if (price === null || price < settings.entryPrice) return;

    // Сброс дневного счётчика по UTC-суткам
    const key = todayKey();
    if (key !== this.tradesTodayKey) {
      this.tradesTodayKey = key;
      this.tradesToday = 0;
    }

    if (settings.dailyLimit !== null && this.tradesToday >= settings.dailyLimit) {
      this.entered.add(market.eventSlug); // не пересматриваем этот рынок повторно
      return;
    }

    // Селект: чередуем через один подходящий момент
    if (this.skipNext) {
      this.skipNext = false;
      this.entered.add(market.eventSlug);
      console.log(`[селект] пропускаем ${market.eventSlug} (через один)`);
      return;
    }
    this.skipNext = true;

    this.entered.add(market.eventSlug);
    this.tradesToday++;
    const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
    this.executeFlip(market, side, tokenId, price);
  }
@@ -127,15 +191,15 @@ class FastFlipBot {
    tokenId: string,
    priceAtEntry: number,
  ): Promise<void> {
    const size = TRADE_SIZE_USD / BUY_PRICE;
    const size = TRADE_SIZE_USD / settings.entryPrice;

    console.log(
      `\n⚡ ВХОД: [${market.coin} / 5мин] "${market.title}"\n` +
        `   Сторона: ${side} | Цена сейчас: ~${priceAtEntry} | Покупаем: ${size.toFixed(2)} акций по ${BUY_PRICE} (~$${TRADE_SIZE_USD})`,
        `   Сторона: ${side} | Цена сейчас: ~${priceAtEntry} | Покупаем: ${size.toFixed(2)} акций по ${settings.entryPrice} (~$${TRADE_SIZE_USD})`,
    );

    if (DRY_RUN || !this.clob) {
      console.log(`   [DRY RUN] Ордер на покупку НЕ отправлен. eventSlug: ${market.eventSlug}`);
      console.log(`   [DRY RUN] Ордер НЕ отправлен. eventSlug: ${market.eventSlug}`);
      return;
    }

@@ -144,42 +208,38 @@ class FastFlipBot {
      const result = await this.clob.placeGtcLimitOrder({
        tokenId,
        side: Side.BUY,
        price: BUY_PRICE,
        price: settings.entryPrice,
        size,
        offsetPct: 0,
      });
      console.log(`   ✅ ЗАЯВКА НА ПОКУПКУ ВЫСТАВЛЕНА: orderId=${result.orderId ?? "?"} status=${result.status}`);
      console.log(`   ✅ ЗАЯВКА НА ПОКУПКУ: orderId=${result.orderId ?? "?"} status=${result.status}`);
      if (!result.orderId) return;
      buyOrderId = result.orderId;
    } catch (err) {
      console.error(`   ❌ ОШИБКА ПОКУПКИ:`, (err as Error).message);
      return;
    }

    // Ждём исполнения (полного или частичного), проверяя статус периодически.
    this.waitForFillThenTakeProfit(market, side, tokenId, buyOrderId, size);
    this.managePosition(market, side, tokenId, buyOrderId, size, settings.entryPrice);
  }

  private async waitForFillThenTakeProfit(
  private async managePosition(
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    tokenId: string,
    buyOrderId: string,
    requestedSize: number,
    buyPrice: number,
  ): Promise<void> {
    if (!this.clob) return;

    // Ждём максимум до конца рынка + небольшой запас — дальше уже не
    // имеет смысла ставить тейк-профит (рынок вот-вот зарезолвится).
    const deadline = market.closeTimeMs + 30 * 1000;

    // Шаг 1: ждём исполнения покупки
    const buyDeadline = market.closeTimeMs + 30 * 1000;
    let filledSize = 0;
    while (Date.now() < deadline) {
    while (Date.now() < buyDeadline) {
      await new Promise((r) => setTimeout(r, FILL_CHECK_INTERVAL_MS));
      try {
        const order = await this.clob.getOrder(buyOrderId);
        // Поле с исполненным объёмом может называться по-разному в
        // зависимости от версии клиента — проверяем несколько вариантов.
        const matched = Number(
          (order as any)?.size_matched ?? (order as any)?.sizeMatched ?? (order as any)?.filledSize ?? 0,
        );
@@ -188,17 +248,18 @@ class FastFlipBot {
          break;
        }
      } catch (err) {
        console.error(`   [ожидание филла] ошибка проверки ордера:`, (err as Error).message);
        console.error(`   [ожидание покупки] ошибка:`, (err as Error).message);
      }
    }

    if (filledSize <= 0) {
      console.log(`   ⏳ Заявка на покупку так и не исполнилась (eventSlug: ${market.eventSlug}) — оставляем висеть, тейк-профит не ставим (нечего продавать).`);
      console.log(`   ⏳ Покупка не исполнилась (eventSlug: ${market.eventSlug}).`);
      return;
    }

    console.log(`   💰 ПОКУПКА ИСПОЛНЕНА: ${filledSize.toFixed(2)} акций. Ставим тейк-профит по ${TP_PRICE}...`);

    let tpOrderId: string | null = null;
    try {
      const tpResult = await this.clob.placeGtcLimitOrder({
        tokenId,
@@ -207,17 +268,102 @@ class FastFlipBot {
        size: filledSize,
        offsetPct: 0,
      });
      console.log(`   ✅ ТЕЙК-ПРОФИТ ВЫСТАВЛЕН: orderId=${tpResult.orderId ?? "?"} status=${tpResult.status}`);
      tpOrderId = tpResult.orderId ?? null;
      console.log(`   ✅ ТЕЙК-ПРОФИТ: orderId=${tpOrderId ?? "?"} status=${tpResult.status}`);
    } catch (err) {
      console.error(`   ❌ ОШИБКА ПОСТАНОВКИ ТЕЙК-ПРОФИТА:`, (err as Error).message);
      console.error(`   ❌ ОШИБКА ТЕЙК-ПРОФИТА:`, (err as Error).message);
    }

    // Шаг 2: ждём исполнения тейк-профита до закрытия рынка (+запас)
    if (tpOrderId) {
      const tpDeadline = market.closeTimeMs + 30 * 1000;
      while (Date.now() < tpDeadline) {
        await new Promise((r) => setTimeout(r, FILL_CHECK_INTERVAL_MS));
        try {
          const order = await this.clob.getOrder(tpOrderId);
          const matched = Number(
            (order as any)?.size_matched ?? (order as any)?.sizeMatched ?? (order as any)?.filledSize ?? 0,
          );
          if (matched >= filledSize - 0.001) {
            const profit = matched * (TP_PRICE - buyPrice);
            await this.notifyClose(market, side, "тейк-профит", profit);
            return;
          }
        } catch (err) {
          console.error(`   [ожидание тейка] ошибка:`, (err as Error).message);
        }
      }
    }

    // Шаг 3: тейк не исполнился — ждём официальный резолв, чтобы узнать итог
    console.log(`   ⏳ Тейк-профит не исполнился до закрытия — ждём резолва (eventSlug: ${market.eventSlug})`);
    await this.waitForResolutionAndNotify(market, side, filledSize, buyPrice);
  }

  private async waitForResolutionAndNotify(
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    filledSize: number,
    buyPrice: number,
  ): Promise<void> {
    for (;;) {
      const waitMs = market.closeTimeMs + RESOLVE_CHECK_DELAY_SEC * 1000 - Date.now();
      if (waitMs > 0) await new Promise((r) => setTimeout(r, waitMs));
      else await new Promise((r) => setTimeout(r, 30 * 1000));

      try {
        const resp = await fetch(`${GAMMA_HOST}/events/slug/${market.eventSlug}`);
        if (!resp.ok) continue;
        const event = await resp.json();
        const m = (event.markets ?? [])[0];
        if (!m) continue;

        let outcomes: string[];
        let outcomePrices: string[];
        try {
          outcomes = JSON.parse(m.outcomes ?? "[]");
          outcomePrices = JSON.parse(m.outcomePrices ?? "[]");
        } catch {
          continue;
        }
        if (outcomes.length !== 2 || outcomePrices.length !== 2) continue;

        const upIdx = outcomes.findIndex((o) => /^up$/i.test(o.trim()));
        const downIdx = outcomes.findIndex((o) => /^down$/i.test(o.trim()));
        if (upIdx === -1 || downIdx === -1) continue;

        const upPrice = Number(outcomePrices[upIdx]);
        const downPrice = Number(outcomePrices[downIdx]);
        if (upPrice > 0.05 && upPrice < 0.95) continue; // ещё не зарезолвился

        const winner: "Up" | "Down" = upPrice > downPrice ? "Up" : "Down";
        const won = side === winner;
        const profit = won ? filledSize * (1 - buyPrice) : -filledSize * buyPrice;
        await this.notifyClose(market, side, won ? "резолв (победа)" : "резолв (проигрыш)", profit);
        return;
      } catch (err) {
        console.error(`   [ожидание резолва] ошибка:`, (err as Error).message);
      }
    }
  }

  private async notifyClose(
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    how: string,
    profit: number,
  ): Promise<void> {
    const sign = profit >= 0 ? "✅" : "🔻";
    const msg = `${sign} Сделка закрыта (${how})\n${market.title}\nСторона: ${side}\nПрофит: ${profit >= 0 ? "+" : ""}$${profit.toFixed(3)}`;
    console.log(`\n${msg}\n`);
    if (this.telegram) await this.telegram.send(msg);
  }

  start(): void {
    this.refreshMarkets();
    setInterval(() => this.refreshMarkets(), MARKET_REFRESH_MS);
    setInterval(() => {
      console.log(`--- статус: апдейтов цены ${this.updateCount}, входов ${this.entered.size} ---`);
      console.log(`--- статус: апдейтов ${this.updateCount}, сделок сегодня ${this.tradesToday} ---`);
    }, 30 * 1000);
  }
}
@@ -251,7 +397,7 @@ async function redeemLoop(): Promise<void> {
    logger,
  );

  console.log("[redeem] Авто-клейм выигрышей запущен, проверка раз в минуту.");
  console.log("[redeem] Авто-клейм выигрышей запущен.");
  const attempted = new Set<string>();

  for (;;) {
@@ -270,9 +416,72 @@ async function redeemLoop(): Promise<void> {
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

        const limitMatch = text.match(/^лимит\s+(\d+|нет)$/);
        if (limitMatch) {
          settings.dailyLimit = limitMatch[1] === "нет" ? null : Number(limitMatch[1]);
          await telegram?.send(`Лимит установлен: ${settings.dailyLimit ?? "нет"}`);
          continue;
        }

        const priceMatch = text.match(/^цена\s+([\d.]+)$/);
        if (priceMatch) {
          settings.entryPrice = Number(priceMatch[1]);
          await telegram?.send(`Цена входа установлена: ${settings.entryPrice}`);
          continue;
        }

        const coinsMatch = text.match(/^монеты\s+([a-zа-я,\s]+)$/i);
        if (coinsMatch) {
          const tickers = coinsMatch[1].split(",").map((s) => s.trim().toUpperCase());
          const coins = tickers.filter((t) => TICKER_TO_COIN[t]).map((t) => TICKER_TO_COIN[t]);
          if (coins.length > 0) {
            settings.coins = coins;
            await telegram?.send(`Монеты установлены: ${tickers.join(", ")}`);
          } else {
            await telegram?.send(`Не распознал монеты. Используй тикеры: BTC, ETH, SOL, XRP, DOGE`);
          }
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
  console.log(`Только Bitcoin, только 5-мин. Покупка по ${BUY_PRICE}, тейк-профит по ${TP_PRICE}, размер $${TRADE_SIZE_USD}`);
  console.log(`Монеты: ${settings.coins.join(", ")} | Цена входа: ${settings.entryPrice} | Лимит: ${settings.dailyLimit ?? "нет"}`);

  let clob: ClobService | null = null;
  if (!DRY_RUN) {
@@ -297,9 +506,17 @@ async function main() {
    console.log("ClobService инициализирован для LIVE торговли.");
  }

  const bot = new FastFlipBot(clob);
  const logger = createLogger(false);
  const telegram = createTelegramNotifier(process.env.TELEGRAM_BOT_TOKEN, process.env.TELEGRAM_CHAT_ID, logger);

  const bot = new FastFlipBot(clob, telegram);
  bot.start();

  if (telegram && process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID) {
    console.log("Telegram-команды включены: лимит N / лимит нет / цена X / монеты BTC,SOL,... / статус");
    pollTelegramCommands(process.env.TELEGRAM_BOT_TOKEN, process.env.TELEGRAM_CHAT_ID, telegram, bot);
  }

  if (!DRY_RUN && AUTO_REDEEM) {
    redeemLoop();
  }