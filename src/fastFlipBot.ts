/**
 * Покупка победившей стороны 5-минутного Crypto Up/Down рынка за 5 секунд до закрытия.
 * Работает отдельно от старого sniperTrader.ts.
 *
 * Логика на рынок:
 *  1. Следим за Up/Down цены через PriceWatcher.
 *  2. Планируем отдельный таймер на closeTime - 5 секунд.
 *  3. В этот момент выбираем сторону, которая ближе всего к 0.99.
 *  4. Ставим GTC BUY ровно по 0.99.
 *  5. Никакого TP/SELL: заявка остаётся активной через закрытие рынка.
 *
 * НАСТРОЙКИ МЕНЯЮТСЯ ЧЕРЕЗ TELEGRAM НА ЛЕТУ (без передеплоя):
 *   лимит 25        — ограничить количество входов в сутки
 *   лимит нет       — снять лимит (без ограничения)
 *   монеты BTC,SOL,ETH,DOGE  — ограничить торговлю выбранными монетами
 *   монеты все       — снова торговать всеми найденными 5-минутными рынками
 *   статус          — текущие настройки + сколько сделок сегодня
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
const MARKET_REFRESH_MS = 30 * 1000;
const OBSERVE_WINDOW_MS = 10 * 60 * 1000;
const ENTRY_LEAD_MS = 5 * 1000;
const ENTRY_PRICE = 0.99;
const MIN_WINNING_PRICE = 0.98;
const REDEEM_POLL_MS = 60 * 1000;

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
  // По умолчанию торгуем ВСЕ рынки, которые вернул discovery.
  // Если FASTFLIP_COINS задан, можно ограничить список.
  dailyLimit:
    process.env.FASTFLIP_DAILY_LIMIT === undefined || process.env.FASTFLIP_DAILY_LIMIT.trim() === ""
      ? null
      : Number(process.env.FASTFLIP_DAILY_LIMIT),
  coins:
    process.env.FASTFLIP_COINS === undefined || process.env.FASTFLIP_COINS.trim() === ""
      ? null
      : process.env.FASTFLIP_COINS
          .split(",")
          .map((s) => s.trim().toUpperCase())
          .filter((t) => TICKER_TO_COIN[t])
          .map((t) => TICKER_TO_COIN[t]),
};

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

function todayKey(): string {
  return new Date().toISOString().slice(0, 10); // YYYY-MM-DD по UTC
}

class FastFlipBot {
  private watcher: PriceWatcher | null = null;
  private tokenIndex = new Map<string, TokenInfo>();
  private lastTokenIds: string[] = [];
  private scheduled = new Set<string>();
  private placed = new Set<string>();
  private updateCount = 0;

  // Последняя известная цена по каждому токену из PriceWatcher.
  // В момент T-5s именно эти цены используются для выбора стороны.
  private lastPrices = new Map<string, number>();
  private closeTimers = new Map<string, NodeJS.Timeout>();

  private tradesToday = 0;
  private tradesTodayKey = todayKey();

  constructor(
    private clob: ClobService | null,
    private telegram: ReturnType<typeof createTelegramNotifier>,
  ) {}

  getStatus(): string {
    return (
      `Режим: BUY @ 0.99 за 5с до закрытия | ` +
      `Лимит: ${settings.dailyLimit ?? "нет"} | Сделок сегодня: ${this.tradesToday}\n` +
      `Размер: $${TRADE_SIZE_USD} | Монеты: ${settings.coins === null ? "ВСЕ ДОСТУПНЫЕ" : settings.coins.map((c) => COIN_TO_TICKER[c] ?? c).join(", ")}`
    );
  }

  /** Логирует ошибку в консоль И шлёт в Telegram. */
  private async notifyError(context: string, err: unknown): Promise<void> {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`   ❌ [${context}]`, message);
    if (this.telegram) {
      try {
        await this.telegram.send(`⚠️ Ошибка (${context}): ${message}`);
      } catch (tgErr) {
        console.error("   [telegram] не удалось отправить сообщение об ошибке:", (tgErr as Error).message);
      }
    }
  }

  async refreshMarkets(): Promise<void> {
    let allMarkets: CryptoUpDownMarket[];
    try {
      allMarkets = await discoverCryptoUpDownMarkets([{ suffixes: ["up-or-down-5m"], minutes: 5 }]);
    } catch (err) {
      await this.notifyError("обновление рынков", err);
      return;
    }

    const now = Date.now();

    // Берём только ещё не закрывшиеся 5-минутные рынки.
    // Окно в 10 минут даёт запас, чтобы успеть подписаться и поставить таймер.
    const markets = allMarkets.filter(
      (m) =>
        (settings.coins === null || settings.coins.includes(m.coin)) &&
        m.closeTimeMs > now &&
        m.closeTimeMs - now <= OBSERVE_WINDOW_MS,
    );

    this.tokenIndex = buildTokenIndex(markets);
    const tokenIds = [...this.tokenIndex.keys()].sort();

    console.log(
      `[refresh] рынков: ${markets.length} (${tokenIds.length} токенов), ` +
        `сделок сегодня: ${this.tradesToday}/${settings.dailyLimit ?? "∞"}`,
    );

    // Подписка на цены нужна только для получения актуальных Up/Down цен.
    const sameAsLastTime =
      tokenIds.length === this.lastTokenIds.length &&
      tokenIds.every((id, i) => id === this.lastTokenIds[i]);

    if (!sameAsLastTime || !this.watcher) {
      this.lastTokenIds = tokenIds;
      this.watcher?.stop();

      if (tokenIds.length === 0) {
        this.watcher = null;
      } else {
        this.watcher = new PriceWatcher(tokenIds, (u) => this.onPriceUpdate(u));
        this.watcher.start();
      }
    }

    // Главное изменение стратегии: для КАЖДОГО рынка ставим отдельный таймер
    // ровно на closeTime - 5 секунд. Не зависим от 30-секундного refresh.
    for (const market of markets) {
      this.scheduleMarket(market);
    }

    // Чистим таймеры рынков, которые уже исчезли из discovery.
    const activeSlugs = new Set(markets.map((m) => m.eventSlug));
    for (const [slug, timer] of this.closeTimers) {
      if (!activeSlugs.has(slug)) {
        clearTimeout(timer);
        this.closeTimers.delete(slug);
      }
    }
  }

  private onPriceUpdate(update: PriceUpdate): void {
    this.updateCount++;
    const info = this.tokenIndex.get(update.tokenId);
    if (!info) return;

    const price = update.bestBid ?? update.bestAsk;
    if (price !== null) {
      this.lastPrices.set(update.tokenId, price);
    }
  }

  private scheduleMarket(market: CryptoUpDownMarket): void {
    if (this.scheduled.has(market.eventSlug) || this.placed.has(market.eventSlug)) return;

    const executeAt = market.closeTimeMs - ENTRY_LEAD_MS;
    const delay = Math.max(0, executeAt - Date.now());

    this.scheduled.add(market.eventSlug);
    console.log(
      `[schedule] ${market.coin} "${market.title}" -> BUY check in ${(delay / 1000).toFixed(1)}s ` +
        `(T-5s)`,
    );

    const timer = setTimeout(() => {
      this.closeTimers.delete(market.eventSlug);
      void this.executeAtClose(market);
    }, delay);

    this.closeTimers.set(market.eventSlug, timer);
  }

  /**
   * Ровно примерно за 5 секунд до closeTime:
   * 1) берём последнюю цену Up и Down;
   * 2) выбираем сторону, которая ближе всего к 0.99;
   * 3) если она действительно "дорогая" (>= 0.98), ставим BUY GTC @ 0.99;
   * 4) ордер НЕ продаём, НЕ заменяем и НЕ отменяем после закрытия рынка.
   */
  private async executeAtClose(market: CryptoUpDownMarket): Promise<void> {
    if (this.placed.has(market.eventSlug)) return;

    // Если из-за задержки Railway/event loop проснулись чуть раньше — ждём
    // до нужного момента. Если позже — выполняем сразу.
    const remaining = market.closeTimeMs - ENTRY_LEAD_MS - Date.now();
    if (remaining > 0) {
      await new Promise((resolve) => setTimeout(resolve, remaining));
    }

    // Сбрасываем дневной счётчик по UTC-суткам.
    const key = todayKey();
    if (key !== this.tradesTodayKey) {
      this.tradesTodayKey = key;
      this.tradesToday = 0;
    }

    if (settings.dailyLimit !== null && this.tradesToday >= settings.dailyLimit) {
      console.log(`   ⛔ Достигнут дневной лимит — ${market.eventSlug} пропущен.`);
      return;
    }

    const upPrice = this.lastPrices.get(market.upTokenId) ?? null;
    const downPrice = this.lastPrices.get(market.downTokenId) ?? null;

    if (upPrice === null && downPrice === null) {
      console.log(`   ⚠️ Нет цены Up/Down за T-5s: ${market.eventSlug}. Пропускаем.`);
      return;
    }

    // Выбираем токен, чья цена ближе всего к 0.99.
    // В нормальном бинарном рынке это и есть текущая "побеждающая" сторона.
    const candidates: Array<{ side: "Up" | "Down"; tokenId: string; price: number }> = [];
    if (upPrice !== null) candidates.push({ side: "Up", tokenId: market.upTokenId, price: upPrice });
    if (downPrice !== null) candidates.push({ side: "Down", tokenId: market.downTokenId, price: downPrice });

    candidates.sort((a, b) => Math.abs(b.price - 0.99) - Math.abs(a.price - 0.99));
    const selected = candidates[candidates.length - 1];

    // Не покупаем рынок, если ни одна сторона не находится около 0.99.
    // Это защита от ситуации, когда websocket давно не обновлялся или рынок
    // находится в необычном состоянии.
    if (selected.price < MIN_WINNING_PRICE) {
      console.log(
        `   ⚠️ ${market.coin} ${market.eventSlug}: Up=${upPrice ?? "?"}, Down=${downPrice ?? "?"}. ` +
          `Нет стороны >= ${MIN_WINNING_PRICE}. Пропускаем.`,
      );
      return;
    }

    this.placed.add(market.eventSlug);
    this.tradesToday++;

    await this.placeWinningSideOrder(market, selected.side, selected.tokenId, selected.price);
  }

  private async placeWinningSideOrder(
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    tokenId: string,
    observedPrice: number,
  ): Promise<void> {
    const buyPrice = ENTRY_PRICE;
    const size = TRADE_SIZE_USD / buyPrice;

    console.log(
      `\n🎯 T-5С BUY: [${market.coin} / 5мин] "${market.title}"\n` +
        `   Up=${this.lastPrices.get(market.upTokenId)?.toFixed(4) ?? "?"} | ` +
        `Down=${this.lastPrices.get(market.downTokenId)?.toFixed(4) ?? "?"}\n` +
        `   Выбрано: ${side} @ ${observedPrice.toFixed(4)} | ` +
        `ставим GTC BUY @ ${buyPrice.toFixed(2)} | ${size.toFixed(2)} shares (~$${TRADE_SIZE_USD})\n` +
        `   ⏳ Ордер оставляем висеть через резолв — TP/SELL отсутствует.`,
    );

    if (DRY_RUN || !this.clob) {
      console.log(`   [DRY RUN] Ордер НЕ отправлен. eventSlug: ${market.eventSlug}`);
      return;
    }

    try {
      const result = await this.clob.placeGtcLimitOrder({
        tokenId,
        side: Side.BUY,
        price: buyPrice,
        size,
        offsetPct: 0,
      });

      console.log(
        `   ✅ GTC BUY выставлен: orderId=${result.orderId ?? "?"} status=${result.status} ` +
          `price=${buyPrice} size=${size.toFixed(2)}`,
      );

      if (!result.orderId) {
        await this.notifyError(
          `покупка (${market.title})`,
          new Error("CLOB вернул результат без orderId"),
        );
      }
    } catch (err) {
      await this.notifyError(`покупка (${market.title})`, err);
    }
  }

  start(): void {
    void this.refreshMarkets();
    setInterval(() => void this.refreshMarkets(), MARKET_REFRESH_MS);
    setInterval(() => {
      console.log(
        `--- статус: апдейтов ${this.updateCount}, запланировано ${this.closeTimers.size}, ` +
          `сделок сегодня ${this.tradesToday} ---`,
      );
    }, 30 * 1000);
  }
}

async function redeemLoop(telegram: ReturnType<typeof createTelegramNotifier>): Promise<void> {
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
      const message = err instanceof Error ? err.message : String(err);
      console.error("[redeem] ошибка:", message);
      if (telegram) {
        try {
          await telegram.send(`⚠️ Ошибка авто-клейма: ${message}`);
        } catch {
          // если и телеграм не отправился — просто идём дальше
        }
      }
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

        const limitMatch = text.match(/^лимит\s+(\d+|нет)$/);
        if (limitMatch) {
          settings.dailyLimit = limitMatch[1] === "нет" ? null : Number(limitMatch[1]);
          await telegram?.send(`Лимит установлен: ${settings.dailyLimit ?? "нет"}`);
          continue;
        }


        const allCoinsMatch = text.match(/^монеты\s+(все|all)$/i);
        if (allCoinsMatch) {
          settings.coins = null;
          await telegram?.send("Монеты: ВСЕ ДОСТУПНЫЕ");
          continue;
        }

        const coinsMatch = text.match(/^монеты\s+([a-zа-я,\s]+)$/i);
        if (coinsMatch) {
          const tickers = coinsMatch[1].split(",").map((s: string) => s.trim().toUpperCase());
          const coins = tickers.filter((t: string) => TICKER_TO_COIN[t]).map((t: string) => TICKER_TO_COIN[t]);
          if (coins.length > 0) {
            settings.coins = coins;
            await telegram?.send(`Монеты установлены: ${tickers.join(", ")}`);
          } else {
            await telegram?.send(`Не распознал монеты. Используй: BTC, ETH, SOL, XRP, DOGE или "монеты все"`);
          }
          continue;
        }
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error("[telegram poll] ошибка:", message);
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

async function main() {
  console.log(`Режим: ${DRY_RUN ? "DRY_RUN (без реальных сделок)" : "⚠️  LIVE — РЕАЛЬНЫЕ ДЕНЬГИ"}`);
  console.log(`Монеты: ${settings.coins === null ? "ВСЕ ДОСТУПНЫЕ" : settings.coins.join(", ")} | BUY цена: ${ENTRY_PRICE} | T-5s | Размер: $${TRADE_SIZE_USD} | Лимит: ${settings.dailyLimit ?? "нет"}`);

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
    console.log("Telegram-команды включены: лимит N / лимит нет / монеты BTC,SOL,... / монеты все / статус");
    pollTelegramCommands(process.env.TELEGRAM_BOT_TOKEN, process.env.TELEGRAM_CHAT_ID, telegram, bot);
  }

  if (!DRY_RUN && AUTO_REDEEM) {
    redeemLoop(telegram);
  }
}

main().catch(async (err) => {
  console.error("Фатальная ошибка:", err);
  process.exit(1);
});