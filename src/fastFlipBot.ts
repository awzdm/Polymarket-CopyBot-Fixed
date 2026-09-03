/**
 * FastFlip BTC 5m — МГНОВЕННАЯ ПОКУПКА @ 0.98
 *
 * Логика:
 * 1. Следим только за BTC 5-minute Up/Down рынками.
 * 2. В реальном времени смотрим bestAsk обоих токенов.
 * 3. Если bestAsk ПЕРВЫЙ РАЗ касается 0.98 (ЛЮБОЕ направление) — 
 *    МГНОВЕННО ставим GTC BUY ровно @ 0.98.
 * 4. После фактического fill BUY сразу ставим GTC SELL ровно @ 0.999
 *    на фактически купленное количество shares.
 * 5. На один market — максимум один вход.
 *
 * FASTFLIP_DRY_RUN=true  — без реальных ордеров.
 * FASTFLIP_DRY_RUN=false — LIVE.
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

const DRY_RUN =
  (process.env.FASTFLIP_DRY_RUN ?? "true").toLowerCase() !== "false";

const AUTO_REDEEM =
  (process.env.FASTFLIP_AUTO_REDEEM ?? "true").toLowerCase() !== "false";

const TRADE_SIZE_USD = Number(
  process.env.FASTFLIP_TRADE_SIZE_USD ?? "5",
);

const MARKET_REFRESH_MS = 30 * 1000;
const REDEEM_POLL_MS = 60 * 1000;

const BUY_TRIGGER_PRICE = 0.98;
const BUY_LIMIT_PRICE = 0.98;
const SELL_LIMIT_PRICE = 0.999;
const FILL_POLL_MS = 500;

interface TokenInfo {
  market: CryptoUpDownMarket;
  side: "Up" | "Down";
}

// 📊 Интерфейс для хранения информации о сделке
interface TradeInfo {
  marketTitle: string;
  side: "Up" | "Down";
  entryTime: Date;
  entryPrice: number;
  exitPrice: number;
  filledSize: number;
  buyOrderId: string;
  sellOrderId: string;
  profit: number;
  exitTime?: Date;
  status: 'pending' | 'closed' | 'failed';
}

function buildTokenIndex(
  markets: CryptoUpDownMarket[],
): Map<string, TokenInfo> {
  const idx = new Map<string, TokenInfo>();

  for (const market of markets) {
    idx.set(market.upTokenId, {
      market,
      side: "Up",
    });

    idx.set(market.downTokenId, {
      market,
      side: "Down",
    });
  }

  return idx;
}

function todayKey(): string {
  return new Date().toISOString().slice(0, 10);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function extractFilledSize(order: any): number {
  const candidates = [
    order?.size_matched,
    order?.sizeMatched,
    order?.filled_size,
    order?.filledSize,
    order?.making_amount,
    order?.makingAmount,
  ];

  for (const value of candidates) {
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) {
      return n;
    }
  }

  return 0;
}

function orderIsFilled(order: any): boolean {
  const status = String(order?.status ?? "").toLowerCase();

  if (
    status === "matched" ||
    status === "filled" ||
    status === "executed"
  ) {
    return true;
  }

  const filled = extractFilledSize(order);
  const original = Number(
    order?.original_size ??
      order?.originalSize ??
      order?.size ??
      order?.quantity ??
      0,
  );

  return filled > 0 && original > 0 && filled >= original - 1e-9;
}

function orderIsTerminalWithoutFill(order: any): boolean {
  const status = String(order?.status ?? "").toLowerCase();

  return (
    status === "cancelled" ||
    status === "canceled" ||
    status === "expired" ||
    status === "rejected"
  );
}

class FastFlipBot {
  private watcher: PriceWatcher | null = null;
  private tokenIndex = new Map<string, TokenInfo>();
  private lastTokenIds: string[] = [];

  // Храним предыдущую цену для каждого токена
  private lastPrices = new Map<string, number>();

  // Один вход максимум на market.
  private triggered = new Set<string>();

  // BUY orderId по market.
  private buyOrders = new Map<string, string>();

  // Защита от параллельной обработки одного market.
  private processing = new Set<string>();

  // 📊 Активные сделки
  private activeTrades = new Map<string, TradeInfo>();

  private updateCount = 0;

  private tradesToday = 0;
  private tradesTodayKey = todayKey();

  constructor(
    private clob: ClobService | null,
    private telegram: ReturnType<typeof createTelegramNotifier>,
  ) {}

  getStatus(): string {
    return (
      `Режим: BTC 5m | BUY @ ${BUY_LIMIT_PRICE.toFixed(2)} -> SELL @ ${SELL_LIMIT_PRICE} | ` +
      `Лимит: ${this.dailyLimitText()} | Сделок сегодня: ${this.tradesToday}\n` +
      `Размер: $${TRADE_SIZE_USD} | DRY_RUN: ${DRY_RUN} | ` +
      `Активных рынков: ${this.tokenIndex.size / 2}`
    );
  }

  private dailyLimitText(): string {
    return String(settings.dailyLimit ?? "нет");
  }

  private resetDailyCounterIfNeeded(): void {
    const key = todayKey();

    if (key !== this.tradesTodayKey) {
      this.tradesTodayKey = key;
      this.tradesToday = 0;
    }
  }

  private async notifyError(
    context: string,
    err: unknown,
  ): Promise<void> {
    const message = err instanceof Error ? err.message : String(err);

    console.error(`❌ [${context}]`, message);

    if (this.telegram) {
      try {
        await this.telegram.send(
          `⚠️ Ошибка (${context}): ${message}`,
        );
      } catch (tgErr) {
        console.error(
          "[telegram] ошибка:",
          (tgErr as Error).message,
        );
      }
    }
  }

  async refreshMarkets(): Promise<void> {
    let allMarkets: CryptoUpDownMarket[];

    try {
      allMarkets = await discoverCryptoUpDownMarkets([
        {
          suffixes: ["up-or-down-5m"],
          minutes: 5,
        },
      ]);
    } catch (err) {
      await this.notifyError("обновление рынков", err);
      return;
    }

    const now = Date.now();

    // ТОЛЬКО BTC и только ещё не закрывшиеся рынки.
    const markets = allMarkets.filter(
      (m) =>
        m.coin === "Bitcoin" &&
        m.closeTimeMs > now,
    );

    this.tokenIndex = buildTokenIndex(markets);

    const tokenIds = [...this.tokenIndex.keys()].sort();

    console.log(
      `[refresh] BTC 5m рынков: ${markets.length}, ` +
        `токенов: ${tokenIds.length}, ` +
        `сделок сегодня: ${this.tradesToday}/${settings.dailyLimit ?? "∞"}`,
    );

    const sameAsLastTime =
      tokenIds.length === this.lastTokenIds.length &&
      tokenIds.every(
        (id, i) => id === this.lastTokenIds[i],
      );

    if (!sameAsLastTime || !this.watcher) {
      this.lastTokenIds = tokenIds;

      this.watcher?.stop();

      // Очищаем историю цен при обновлении рынков
      this.lastPrices.clear();

      if (tokenIds.length === 0) {
        this.watcher = null;
        return;
      }

      this.watcher = new PriceWatcher(
        tokenIds,
        (update) => this.onPriceUpdate(update),
      );

      this.watcher.start();

      console.log(
        `[watcher] подписка на ${tokenIds.length} BTC Up/Down токенов`,
      );
    }
  }

  // НОВАЯ ЛОГИКА: Мгновенная покупка при ПЕРВОМ касании 0.98 (ЛЮБОЕ направление)
  private onPriceUpdate(update: PriceUpdate): void {
    this.updateCount++;

    const info = this.tokenIndex.get(update.tokenId);
    if (!info) return;

    const bestAsk = update.bestAsk;
    if (bestAsk === null || bestAsk === undefined) {
      return;
    }

    const market = info.market;
    const marketKey = market.eventSlug;

    // Если уже был вход на этот рынок - игнорируем
    if (this.triggered.has(marketKey)) {
      return;
    }

    // Проверяем, не в обработке ли уже
    if (this.processing.has(marketKey)) {
      return;
    }

    // Получаем предыдущую цену для проверки первого касания
    const previousAsk = this.lastPrices.get(update.tokenId);
    this.lastPrices.set(update.tokenId, bestAsk);

    // ⚡ ПРОВЕРКА: ПЕРВОЕ КАСАНИЕ 0.98 (ЛЮБОЕ НАПРАВЛЕНИЕ)
    // Цена СТАЛА 0.98, а ДО ЭТОГО была НЕ 0.98 (или undefined)
    const isFirstTouch = (previousAsk === undefined || previousAsk !== 0.98) && 
                         bestAsk === 0.98;

    // Если это не первое касание 0.98 - игнорируем
    if (!isFirstTouch) {
      // Логируем для отладки
      if (bestAsk === 0.98 && previousAsk === 0.98) {
        console.log(
          `⏸️ [${market.title}] Уже было касание 0.98, пропускаем (повтор)`
        );
      }
      return;
    }

    // --- ПЕРВОЕ КАСАНИЕ 0.98 (ЛЮБОЕ НАПРАВЛЕНИЕ) ---
    console.log(
      `\n🎯 ПЕРВОЕ КАСАНИЕ 0.98! ${market.title}\n` +
        `   Side: ${info.side}\n` +
        `   Было: ${previousAsk?.toFixed(4) ?? "неизвестно"}\n` +
        `   Стало: ${bestAsk.toFixed(4)} === 0.98\n` +
        `   🚀 ПОКУПАЕМ МГНОВЕННО!\n`,
    );

    // ❌ УБИРАЕМ УВЕДОМЛЕНИЕ О ВХОДЕ
    // if (this.telegram) {
    //   void this.telegram.send(
    //     `🚀 FASTFLIP МГНОВЕННЫЙ ВХОД!\n` +
    //     `${market.title}\n` +
    //     `${info.side}\n` +
    //     `Цена: ${bestAsk.toFixed(4)}\n` +
    //     `Покупка @ 0.98 СРАЗУ!`
    //   );
    // }

    // Проверяем дневной лимит
    this.resetDailyCounterIfNeeded();
    if (settings.dailyLimit !== null && this.tradesToday >= settings.dailyLimit) {
      console.log(
        `⛔ Дневной лимит достигнут. Пропускаем ${marketKey}`,
      );
      if (this.telegram) {
        void this.telegram.send(
          `⛔ Дневной лимит достигнут!\n` +
          `${market.title}\n` +
          `Сделок сегодня: ${this.tradesToday}`
        );
      }
      return;
    }

    // Мгновенно покупаем (без таймера)
    this.triggered.add(marketKey);
    this.processing.add(marketKey);

    void this.buyAndPlaceTakeProfit(
      info.market,
      info.side,
      update.tokenId,
    ).finally(() => {
      this.processing.delete(marketKey);
    });
  }

  private async buyAndPlaceTakeProfit(
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    tokenId: string,
  ): Promise<void> {
    const marketKey = market.eventSlug;

    const size = TRADE_SIZE_USD / BUY_LIMIT_PRICE;

    console.log(
      `🟢 BUY ${side}\n` +
        `   market: ${market.title}\n` +
        `   price: ${BUY_LIMIT_PRICE}\n` +
        `   size: ${size.toFixed(6)} shares\n` +
        `   value: ~$${TRADE_SIZE_USD}\n`,
    );

    if (DRY_RUN || !this.clob) {
      console.log(
        `🧪 [DRY RUN] BUY НЕ отправлен: ${marketKey}`,
      );
      return;
    }

    try {
      const buyResult =
        await this.clob.placeExactGtcLimitOrder({
          tokenId,
          side: Side.BUY,
          price: BUY_LIMIT_PRICE,
          size,
        });

      const buyOrderId = buyResult.orderId;

      if (!buyOrderId) {
        throw new Error(
          "CLOB вернул BUY без orderId",
        );
      }

      this.buyOrders.set(marketKey, buyOrderId);

      console.log(
        `✅ BUY LIMIT выставлен\n` +
          `   orderId: ${buyOrderId}\n` +
          `   price: ${BUY_LIMIT_PRICE}\n` +
          `   size: ${size.toFixed(6)}\n` +
          `   Ждём фактический fill...`,
      );

      const filledSize =
        await this.waitForBuyFill(
          buyOrderId,
          size,
          market.title,
        );

      if (filledSize <= 0) {
        throw new Error(
          "BUY order завершился без заполненного количества",
        );
      }

      console.log(
        `\n💰 BUY FILLED\n` +
          `   orderId: ${buyOrderId}\n` +
          `   filled: ${filledSize.toFixed(6)} shares\n` +
          `   Теперь SELL @ ${SELL_LIMIT_PRICE}\n`,
      );

      // Считаем сделку только после реального BUY fill.
      this.resetDailyCounterIfNeeded();
      this.tradesToday++;

      const sellResult =
        await this.clob.placeExactGtcLimitOrder({
          tokenId,
          side: Side.SELL,
          price: SELL_LIMIT_PRICE,
          size: filledSize,
        });

      const sellOrderId = sellResult.orderId ?? "?";

      console.log(
        `✅ SELL LIMIT выставлен\n` +
          `   orderId: ${sellOrderId}\n` +
          `   price: ${SELL_LIMIT_PRICE}\n` +
          `   size: ${filledSize.toFixed(6)} shares\n`,
      );

      // ❌ УБИРАЕМ УВЕДОМЛЕНИЕ О ВЫСТАВЛЕНИИ SELL
      // if (this.telegram) {
      //   await this.telegram.send(
      //     `✅ FastFlip BTC\n` +
      //     `${market.title}\n` +
      //     `${side}\n` +
      //     `BUY ${filledSize.toFixed(4)} @ ${BUY_LIMIT_PRICE}\n` +
      //     `SELL @ ${SELL_LIMIT_PRICE}\n` +
      //     `BUY order: ${buyOrderId}\n` +
      //     `SELL order: ${sellResult.orderId ?? "?"}`
      //   );
      // }

      // 📊 СОЗДАЕМ ЗАПИСЬ О СДЕЛКЕ
      const tradeInfo: TradeInfo = {
        marketTitle: market.title,
        side: side,
        entryTime: new Date(),
        entryPrice: BUY_LIMIT_PRICE,
        exitPrice: SELL_LIMIT_PRICE,
        filledSize: filledSize,
        buyOrderId: buyOrderId,
        sellOrderId: sellOrderId,
        profit: 0,
        status: 'pending'
      };
      
      this.activeTrades.set(marketKey, tradeInfo);

      // 🔍 ЗАПУСКАЕМ МОНИТОРИНГ SELL ОРДЕРА
      await this.monitorSellOrder(marketKey, tradeInfo);

    } catch (err) {
      await this.notifyError(
        `FastFlip ${market.title}`,
        err,
      );
      
      // Отправляем сообщение об ошибке
      if (this.telegram) {
        await this.telegram.send(
          `❌ ОШИБКА В СДЕЛКЕ!\n` +
          `${market.title}\n` +
          `${side}\n` +
          `Ошибка: ${err instanceof Error ? err.message : String(err)}`
        );
      }
    }
  }

  // 🔍 НОВЫЙ МЕТОД: Мониторинг SELL ордера
  private async monitorSellOrder(
    marketKey: string,
    tradeInfo: TradeInfo
  ): Promise<void> {
    if (!this.clob) return;

    for (;;) {
      await sleep(FILL_POLL_MS);
      
      try {
        const order = await this.clob.getOrder(tradeInfo.sellOrderId);
        
        if (orderIsFilled(order)) {
          // 🎉 СДЕЛКА ЗАКРЫТА!
          tradeInfo.exitTime = new Date();
          tradeInfo.status = 'closed';
          
          // Считаем профит
          const profit = tradeInfo.filledSize * (SELL_LIMIT_PRICE - BUY_LIMIT_PRICE);
          tradeInfo.profit = profit;
          
          // Форматируем время
          const entryTimeStr = tradeInfo.entryTime.toLocaleString('ru-RU', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
          });
          
          const exitTimeStr = tradeInfo.exitTime.toLocaleString('ru-RU', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
          });
          
          // Время в сделке
          const durationMs = tradeInfo.exitTime.getTime() - tradeInfo.entryTime.getTime();
          const durationSec = Math.floor(durationMs / 1000);
          const durationStr = durationSec < 60 
            ? `${durationSec} сек`
            : `${Math.floor(durationSec / 60)} мин ${durationSec % 60} сек`;
          
          // 💬 ОТПРАВЛЯЕМ УВЕДОМЛЕНИЕ В TELEGRAM
          if (this.telegram) {
            const profitEmoji = profit > 0 ? '✅' : '❌';
            const profitPercent = ((SELL_LIMIT_PRICE - BUY_LIMIT_PRICE) / BUY_LIMIT_PRICE * 100).toFixed(2);
            
            await this.telegram.send(
              `${profitEmoji} СДЕЛКА ЗАКРЫТА!\n` +
              `━━━━━━━━━━━━━━━━━━━━\n` +
              `📊 ${tradeInfo.marketTitle}\n` +
              `🎯 Направление: ${tradeInfo.side}\n` +
              `━━━━━━━━━━━━━━━━━━━━\n` +
              `⏰ ВХОД: ${entryTimeStr}\n` +
              `⏰ ВЫХОД: ${exitTimeStr}\n` +
              `⏱️ Длительность: ${durationStr}\n` +
              `━━━━━━━━━━━━━━━━━━━━\n` +
              `📈 Вход: ${tradeInfo.entryPrice.toFixed(4)}\n` +
              `📉 Выход: ${tradeInfo.exitPrice.toFixed(4)}\n` +
              `📦 Объём: ${tradeInfo.filledSize.toFixed(4)} shares\n` +
              `━━━━━━━━━━━━━━━━━━━━\n` +
              `💰 ПРОФИТ: $${profit.toFixed(4)} (${profitPercent}%)\n` +
              `🆔 BUY: ${tradeInfo.buyOrderId.substring(0, 8)}...\n` +
              `🆔 SELL: ${tradeInfo.sellOrderId.substring(0, 8)}...`
            );
          }
          
          // Логируем в консоль
          console.log(
            `\n🎉 СДЕЛКА ЗАКРЫТА!\n` +
            `   ${tradeInfo.marketTitle}\n` +
            `   Вход: ${entryTimeStr}\n` +
            `   Выход: ${exitTimeStr}\n` +
            `   Длительность: ${durationStr}\n` +
            `   Профит: $${profit.toFixed(4)}\n`
          );
          
          // Удаляем из активных сделок
          this.activeTrades.delete(marketKey);
          break;
        }
        
        if (orderIsTerminalWithoutFill(order)) {
          // ⚠️ SELL НЕ ИСПОЛНИЛСЯ
          tradeInfo.status = 'failed';
          tradeInfo.exitTime = new Date();
          
          if (this.telegram) {
            await this.telegram.send(
              `⚠️ SELL НЕ ИСПОЛНИЛСЯ!\n` +
              `${tradeInfo.marketTitle}\n` +
              `Направление: ${tradeInfo.side}\n` +
              `Статус: ${order?.status ?? '?'}\n` +
              `BUY ордер: ${tradeInfo.buyOrderId}\n` +
              `SELL ордер: ${tradeInfo.sellOrderId}`
            );
          }
          
          this.activeTrades.delete(marketKey);
          break;
        }
        
      } catch (err) {
        console.error(`[monitorSellOrder] ошибка:`, err);
        // Не прерываем цикл при ошибке, продолжаем мониторить
        await sleep(1000);
      }
    }
  }

  private async waitForBuyFill(
    orderId: string,
    requestedSize: number,
    title: string,
  ): Promise<number> {
    if (!this.clob) {
      throw new Error("CLOB service не инициализирован");
    }

    for (;;) {
      const order = await this.clob.getOrder(orderId);

      const filledSize = extractFilledSize(order);

      if (filledSize > 0) {
        console.log(
          `[fill] ${title}: ` +
            `${filledSize.toFixed(6)}/${requestedSize.toFixed(6)}`,
        );
      }

      if (orderIsFilled(order)) {
        return filledSize > 0 ? filledSize : requestedSize;
      }

      if (orderIsTerminalWithoutFill(order)) {
        throw new Error(
          `BUY order ${orderId} завершён без fill. ` +
            `status=${String(order?.status ?? "?")}`,
        );
      }

      await sleep(FILL_POLL_MS);
    }
  }

  start(): void {
    void this.refreshMarkets();

    setInterval(
      () => void this.refreshMarkets(),
      MARKET_REFRESH_MS,
    );

    setInterval(() => {
      console.log(
        `--- status: updates=${this.updateCount}, ` +
          `BTC markets=${this.tokenIndex.size / 2}, ` +
          `triggered=${this.triggered.size}, ` +
          `processing=${this.processing.size}, ` +
          `tradesToday=${this.tradesToday}, ` +
          `activeTrades=${this.activeTrades.size} ---`,
      );
    }, 30 * 1000);
  }
}

// ─── Настройки ──────────────────────────────────────────────

const settings = {
  dailyLimit:
    process.env.FASTFLIP_DAILY_LIMIT === undefined ||
    process.env.FASTFLIP_DAILY_LIMIT.trim() === ""
      ? null
      : Number(process.env.FASTFLIP_DAILY_LIMIT),
};

// ─── Auto redeem ────────────────────────────────────────────

async function redeemLoop(
  telegram: ReturnType<typeof createTelegramNotifier>,
): Promise<void> {
  const rpcUrl = process.env.RPC_URL;
  const profileAddress =
    process.env.PROFILE_ADDRESS ??
    process.env.FUNDER_ADDRESS;

  if (!rpcUrl || !profileAddress) {
    console.warn(
      "[redeem] RPC_URL или PROFILE_ADDRESS не заданы — " +
        "авто-клейм отключён.",
    );
    return;
  }

  const logger = createLogger(false);

  const dataApi = new DataApiClient(
    process.env.DATA_API_HOST ??
      "https://data-api.polymarket.com",
    logger,
  );

  const apiKey = process.env.BUILDER_API_KEY;
  const apiSecret = process.env.BUILDER_API_SECRET;
  const apiPassphrase =
    process.env.BUILDER_API_PASSPHRASE;

  const builderCreds =
    apiKey && apiSecret && apiPassphrase
      ? {
          key: apiKey,
          secret: apiSecret,
          passphrase: apiPassphrase,
        }
      : undefined;

  const redeemService = RedeemService.init(
    {
      relayerUrl:
        process.env.RELAYER_URL ??
        "https://relayer-v2.polymarket.com",
      chainId: Number(
        process.env.CHAIN_ID ?? "137",
      ),
      privateKey: process.env.PRIVATE_KEY!,
      rpcUrl,
      txType:
        (process.env.RELAYER_TX_TYPE as
          | "SAFE"
          | "PROXY") ?? "PROXY",
      builderCreds,
    },
    logger,
  );

  console.log(
    "[redeem] Авто-клейм выигрышей запущен.",
  );

  const attempted = new Set<string>();

  for (;;) {
    try {
      const positions =
        await dataApi.getPositions(
          profileAddress,
          true,
        );

      const eligible = positions.filter(
        (p) =>
          !attempted.has(p.conditionId),
      );

      if (eligible.length > 0) {
        const txHashes =
          await redeemService.redeemPositions(
            eligible,
          );

        for (const p of eligible) {
          attempted.add(p.conditionId);
        }

        if (txHashes.length > 0) {
          console.log(
            `[redeem] ✅ Заклеймлено: ${txHashes.length}`,
            txHashes,
          );
        }
      }
    } catch (err) {
      const message =
        err instanceof Error
          ? err.message
          : String(err);

      console.error(
        "[redeem] ошибка:",
        message,
      );

      if (telegram) {
        try {
          await telegram.send(
            `⚠️ Ошибка авто-клейма: ${message}`,
          );
        } catch {
          // ignore
        }
      }
    }

    await sleep(REDEEM_POLL_MS);
  }
}

// ─── Telegram ───────────────────────────────────────────────

async function pollTelegramCommands(
  botToken: string,
  chatId: string,
  telegram: ReturnType<typeof createTelegramNotifier>,
  bot: FastFlipBot,
): Promise<void> {
  let offset = 0;

  const apiUrl =
    `https://api.telegram.org/bot${botToken}/getUpdates`;

  for (;;) {
    try {
      const resp = await fetch(
        `${apiUrl}?offset=${offset}&timeout=25`,
      );

      if (!resp.ok) {
        await sleep(5000);
        continue;
      }

      const data = await resp.json();

      for (const update of data.result ?? []) {
        offset = update.update_id + 1;

        const msg = update.message;

        if (
          !msg?.text ||
          String(msg.chat?.id) !==
            String(chatId)
        ) {
          continue;
        }

        const text =
          msg.text.trim().toLowerCase();

        if (text === "статус") {
          await telegram?.send(
            bot.getStatus(),
          );
          continue;
        }

        const limitMatch =
          text.match(/^лимит\s+(\d+|нет)$/);

        if (limitMatch) {
          settings.dailyLimit =
            limitMatch[1] === "нет"
              ? null
              : Number(limitMatch[1]);

          await telegram?.send(
            `Лимит установлен: ` +
              `${settings.dailyLimit ?? "нет"}`,
          );

          continue;
        }
      }
    } catch (err) {
      const message =
        err instanceof Error
          ? err.message
          : String(err);

      console.error(
        "[telegram poll] ошибка:",
        message,
      );

      await sleep(5000);
    }
  }
}

// ─── Main ───────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log(
    `\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
  );
  console.log(
    ` FASTFLIP BTC 5M — МГНОВЕННЫЙ ВХОД`,
  );
  console.log(
    ` BUY @ ${BUY_LIMIT_PRICE} -> SELL @ ${SELL_LIMIT_PRICE}`,
  );
  console.log(
    ` Trigger: ПЕРВОЕ касание bestAsk = ${BUY_TRIGGER_PRICE} (ЛЮБОЕ направление)`,
  );
  console.log(
    ` ⚡ ПОКУПКА МГНОВЕННО, без подтверждения!`,
  );
  console.log(
    ` Mode: ${
      DRY_RUN
        ? "🧪 DRY_RUN — без реальных сделок"
        : "⚠️ LIVE — РЕАЛЬНЫЕ ДЕНЬГИ"
    }`,
  );
  console.log(
    ` Size: $${TRADE_SIZE_USD}`,
  );
  console.log(
    `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`,
  );

  let clob: ClobService | null = null;

  if (!DRY_RUN) {
    const logger = createLogger(false);

    const apiKey =
      process.env.CLOB_API_KEY;

    const apiSecret =
      process.env.CLOB_API_SECRET;

    const apiPassphrase =
      process.env.CLOB_API_PASSPHRASE;

    const apiCreds =
      apiKey &&
      apiSecret &&
      apiPassphrase
        ? {
            key: apiKey,
            secret: apiSecret,
            passphrase: apiPassphrase,
          }
        : undefined;

    clob = await ClobService.init(
      {
        host:
          process.env.CLOB_HOST ??
          "https://clob.polymarket.com",

        rpcUrl: process.env.RPC_URL,

        chainId: Number(
          process.env.CHAIN_ID ?? "137",
        ),

        privateKey:
          process.env.PRIVATE_KEY!,

        signatureType: Number(
          process.env.SIGNATURE_TYPE ?? "1",
        ),

        funderAddress:
          process.env.FUNDER_ADDRESS ??
          process.env.PROFILE_ADDRESS,

        apiCreds,
      },
      logger,
    );

    console.log(
      "ClobService инициализирован для LIVE торговли.",
    );
  }

  const logger = createLogger(false);

  const telegram =
    createTelegramNotifier(
      process.env.TELEGRAM_BOT_TOKEN,
      process.env.TELEGRAM_CHAT_ID,
      logger,
    );

  const bot = new FastFlipBot(
    clob,
    telegram,
  );

  bot.start();

  if (
    telegram &&
    process.env.TELEGRAM_BOT_TOKEN &&
    process.env.TELEGRAM_CHAT_ID
  ) {
    console.log(
      "Telegram: статус / лимит N / лимит нет",
    );

    void pollTelegramCommands(
      process.env.TELEGRAM_BOT_TOKEN,
      process.env.TELEGRAM_CHAT_ID,
      telegram,
      bot,
    );
  }

  if (!DRY_RUN && AUTO_REDEEM) {
    void redeemLoop(telegram);
  }
}

main().catch(async (err) => {
  console.error(
    "Фатальная ошибка:",
    err,
  );

  process.exit(1);
});