/**
 * "Быстрый флип" — отдельная, ПАРАЛЛЕЛЬНАЯ стратегия, не связана с
 * sniperTrader.ts (тот пока просто не запускаем, код не трогаем).
 *
 * Только Bitcoin, только 5-минутные рынки.
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
 *
 * DRY_RUN=true по умолчанию — только логи, без реальных денег, пока не
 * выставишь FASTFLIP_DRY_RUN=false явно.
 */

import "dotenv/config";
import { Side } from "@polymarket/clob-client-v2";
import { discoverCryptoUpDownMarkets, CryptoUpDownMarket } from "./cryptoMarketDiscovery.js";
import { PriceWatcher, PriceUpdate } from "./priceWatcher.js";
import { ClobService } from "./clob.js";
import { RedeemService } from "./redeem.js";
import { DataApiClient } from "./dataApi.js";
import { createLogger } from "./logger.js";

const DRY_RUN = (process.env.FASTFLIP_DRY_RUN ?? "true").toLowerCase() !== "false";
const AUTO_REDEEM = (process.env.FASTFLIP_AUTO_REDEEM ?? "true").toLowerCase() !== "false";
const TRADE_SIZE_USD = Number(process.env.FASTFLIP_TRADE_SIZE_USD ?? "5");
const BUY_PRICE = Number(process.env.FASTFLIP_BUY_PRICE ?? "0.99");
const TP_PRICE = Number(process.env.FASTFLIP_TP_PRICE ?? "0.999");
const MARKET_REFRESH_MS = 30 * 1000;
const OBSERVE_WINDOW_MS = 6 * 60 * 1000; // весь 5-мин рынок с запасом
const FILL_CHECK_INTERVAL_MS = 5 * 1000;
const REDEEM_POLL_MS = 60 * 1000;

const TARGET_COIN = "Bitcoin";

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

class FastFlipBot {
  private watcher: PriceWatcher | null = null;
  private tokenIndex = new Map<string, TokenInfo>();
  private lastTokenIds: string[] = [];
  private entered = new Set<string>(); // eventSlug — уже вошли в этот рынок
  private updateCount = 0;

  constructor(private clob: ClobService | null) {}

  async refreshMarkets(): Promise<void> {
    let allMarkets: CryptoUpDownMarket[];
    try {
      allMarkets = await discoverCryptoUpDownMarkets([{ suffixes: ["up-or-down-5m"], minutes: 5 }]);
    } catch (err) {
      console.error("[refresh] ошибка:", (err as Error).message);
      return;
    }

    const now = Date.now();
    const markets = allMarkets.filter(
      (m) => m.coin === TARGET_COIN && m.closeTimeMs - now <= OBSERVE_WINDOW_MS,
    );

    this.tokenIndex = buildTokenIndex(markets);
    const tokenIds = [...this.tokenIndex.keys()].sort();

    console.log(
      `[refresh] Bitcoin 5-мин рынков: ${markets.length} (${tokenIds.length} токенов), входов сделано: ${this.entered.size}`,
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
    const info = this.tokenIndex.get(update.tokenId);
    if (!info) return;
    const { market, side } = info;

    if (this.entered.has(market.eventSlug)) return;

    const price = update.bestBid ?? update.bestAsk;
    if (price === null || price < BUY_PRICE) return;

    this.entered.add(market.eventSlug);
    const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
    this.executeFlip(market, side, tokenId, price);
  }

  private async executeFlip(
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    tokenId: string,
    priceAtEntry: number,
  ): Promise<void> {
    const size = TRADE_SIZE_USD / BUY_PRICE;

    console.log(
      `\n⚡ ВХОД: [${market.coin} / 5мин] "${market.title}"\n` +
        `   Сторона: ${side} | Цена сейчас: ~${priceAtEntry} | Покупаем: ${size.toFixed(2)} акций по ${BUY_PRICE} (~$${TRADE_SIZE_USD})`,
    );

    if (DRY_RUN || !this.clob) {
      console.log(`   [DRY RUN] Ордер на покупку НЕ отправлен. eventSlug: ${market.eventSlug}`);
      return;
    }

    let buyOrderId: string;
    try {
      const result = await this.clob.placeGtcLimitOrder({
        tokenId,
        side: Side.BUY,
        price: BUY_PRICE,
        size,
        offsetPct: 0,
      });
      console.log(`   ✅ ЗАЯВКА НА ПОКУПКУ ВЫСТАВЛЕНА: orderId=${result.orderId ?? "?"} status=${result.status}`);
      if (!result.orderId) return;
      buyOrderId = result.orderId;
    } catch (err) {
      console.error(`   ❌ ОШИБКА ПОКУПКИ:`, (err as Error).message);
      return;
    }

    // Ждём исполнения (полного или частичного), проверяя статус периодически.
    this.waitForFillThenTakeProfit(market, side, tokenId, buyOrderId, size);
  }

  private async waitForFillThenTakeProfit(
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    tokenId: string,
    buyOrderId: string,
    requestedSize: number,
  ): Promise<void> {
    if (!this.clob) return;

    // Ждём максимум до конца рынка + небольшой запас — дальше уже не
    // имеет смысла ставить тейк-профит (рынок вот-вот зарезолвится).
    const deadline = market.closeTimeMs + 30 * 1000;

    let filledSize = 0;
    while (Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, FILL_CHECK_INTERVAL_MS));
      try {
        const order = await this.clob.getOrder(buyOrderId);
        // Поле с исполненным объёмом может называться по-разному в
        // зависимости от версии клиента — проверяем несколько вариантов.
        const matched = Number(
          (order as any)?.size_matched ?? (order as any)?.sizeMatched ?? (order as any)?.filledSize ?? 0,
        );
        if (matched > 0) {
          filledSize = matched;
          break;
        }
      } catch (err) {
        console.error(`   [ожидание филла] ошибка проверки ордера:`, (err as Error).message);
      }
    }

    if (filledSize <= 0) {
      console.log(`   ⏳ Заявка на покупку так и не исполнилась (eventSlug: ${market.eventSlug}) — оставляем висеть, тейк-профит не ставим (нечего продавать).`);
      return;
    }

    console.log(`   💰 ПОКУПКА ИСПОЛНЕНА: ${filledSize.toFixed(2)} акций. Ставим тейк-профит по ${TP_PRICE}...`);

    try {
      const tpResult = await this.clob.placeGtcLimitOrder({
        tokenId,
        side: Side.SELL,
        price: TP_PRICE,
        size: filledSize,
        offsetPct: 0,
      });
      console.log(`   ✅ ТЕЙК-ПРОФИТ ВЫСТАВЛЕН: orderId=${tpResult.orderId ?? "?"} status=${tpResult.status}`);
    } catch (err) {
      console.error(`   ❌ ОШИБКА ПОСТАНОВКИ ТЕЙК-ПРОФИТА:`, (err as Error).message);
    }
  }

  start(): void {
    this.refreshMarkets();
    setInterval(() => this.refreshMarkets(), MARKET_REFRESH_MS);
    setInterval(() => {
      console.log(`--- статус: апдейтов цены ${this.updateCount}, входов ${this.entered.size} ---`);
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

  console.log("[redeem] Авто-клейм выигрышей запущен, проверка раз в минуту.");
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

async function main() {
  console.log(`Режим: ${DRY_RUN ? "DRY_RUN (без реальных сделок)" : "⚠️  LIVE — РЕАЛЬНЫЕ ДЕНЬГИ"}`);
  console.log(`Только Bitcoin, только 5-мин. Покупка по ${BUY_PRICE}, тейк-профит по ${TP_PRICE}, размер $${TRADE_SIZE_USD}`);

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

  const bot = new FastFlipBot(clob);
  bot.start();

  if (!DRY_RUN && AUTO_REDEEM) {
    redeemLoop();
  }
}

main().catch((err) => {
  console.error("Фатальная ошибка:", err);
  process.exit(1);
});