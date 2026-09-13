/**
 * "Быстрый флип" v5.2 — РЫНОЧНЫЕ ордера, вход только в последние секунды
 * перед закрытием, С квотой сделок в час, БЕЗ заранее выбранной цели.
 * ПАРАЛЛЕЛЬНЫЙ старым sniperTrader.ts, fastFlip.ts и fastFlipMarketBot.v5.1.ts
 * (их не трогаем, не запускаем).
 *
 * ЧТО НОВОГО В v5.2 (относительно v5.1):
 *
 *   1. ЧЕСТНАЯ МАКСИМАЛЬНАЯ ПРОСАДКА. В v5.1 просадка считалась по
 *      ЛЮБОЙ доступной цене (bestBid ?? bestAsk), из-за чего в последние
 *      секунды перед закрытием окна, когда маркет-мейкеры убирают
 *      ликвидность и в стакане остаётся один "мусорный" огрызок
 *      (например, забытый лимитник по 0.01), бот записывал это как
 *      "цена упала на 99%" — хотя по факту никто по такой цене не
 *      торговал и реального движения не было.
 *
 *      Теперь просадка обновляется ТОЛЬКО когда:
 *        а) в стакане одновременно есть И бид, И аск (значит рынок
 *           живой, двусторонний, а не пустой хвост), И
 *        б) до закрытия текущего окна осталось больше
 *           DRAWDOWN_IGNORE_LAST_SEC секунд (по умолчанию 10) — в
 *           последний момент перед резолвом ликвидность почти всегда
 *           исчезает, и туда лезть незачем.
 *
 *   2. ДВИЖЕНИЕ ЦЕНЫ BTC ОТ НАЧАЛА ОКНА ДО МОМЕНТА ВХОДА. При входе в
 *      сделку бот теперь параллельно с ордером запрашивает текущую цену
 *      BTC/USD через Chainlink price feed на Polygon (тот же RPC_URL,
 *      что уже используется для редима — никаких новых ключей/api не
 *      нужно). Цену на МОМЕНТ ОТКРЫТИЯ КАЖДОГО 5-минутного окна бот
 *      запоминает заранее — таймером, выставленным точно на время
 *      открытия окна (closeTimeMs - windowMinutes). В момент входа
 *      считается % изменения цены BTC от начала окна до входа, и эта
 *      цифра добавляется в итоговый отчёт по сделке в Telegram
 *      ("во время входа цена BTC от начала была +0.42%").
 *
 *      Если по какой-то причине Chainlink-цену не удалось получить
 *      (нет RPC_URL, RPC недоступен и т.п.) — строка в отчёте просто не
 *      добавляется, вся остальная логика бота при этом не блокируется
 *      и не замедляется (цена BTC запрашивается НЕ последовательно
 *      перед ордером, а параллельно с ним).
 *
 * ВСЁ ОСТАЛЬНОЕ (сетап входа, квота в час, рыночные ордера через
 * ClobService.placeLimitOrder, отсутствие стоп-лосса, резолв через
 * Gamma API, Telegram-команды) — без изменений, см. комментарии внутри.
 *
 * НАСТРОЙКИ НА ЛЕТУ ЧЕРЕЗ TELEGRAM (без передеплоя):
 *   цена 0.98     — цена входа
 *   тейк 0.99     — цена выхода
 *   окно 30       — сколько секунд до закрытия окна разрешён вход
 *   квота 3       — сколько сделок максимум за текущий час
 *   статус        — текущие настройки + что происходит сейчас
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
// открытой позиции и не идёт попытка входа — см. refreshMarkets).
const MARKET_REFRESH_MS = 15 * 1000;

// На сколько вперёд видим рынок, чтобы начать за ним следить.
function observeWindowMs(windowMinutes: number): number {
  return (windowMinutes + 1) * 60 * 1000;
}

// Буфер поверх живой цены стакана (в процентах) — передаётся в реальный
// метод ClobService.placeLimitOrder как maxSlippagePct.
const MARKET_ORDER_SLIPPAGE_PCT = Number(process.env.FASTFLIP_SLIPPAGE_PCT ?? "0.5");

// Минимальный запас прибыли (в долях цены), без которого выход не имеет
// смысла — если покупка уже съела весь запас проскальзыванием, просто
// держим позицию до официального резолва вместо мгновенного выхода в ноль.
const MIN_PROFIT_MARGIN = Number(process.env.FASTFLIP_MIN_PROFIT_MARGIN ?? "0.003");

// Если реальная цена покупки оказалась НАМНОГО ниже ожидаемой (entryPrice)
// — значит цена рухнула ПРЯМО ПОКА ордер летел до биржи (рынок начал
// резкий разворот в тот же момент). Это больше не "почти гарантированная"
// позиция по 0.98, а фактически монетка на исход. В этом случае сразу
// пробуем аварийно продать обратно вместо того, чтобы слепо держать до
// резолва — см. executeMarketEntry/emergencyExit.
const BAD_FILL_TOLERANCE = Number(process.env.FASTFLIP_BAD_FILL_TOLERANCE ?? "0.02");

// После закрытия окна ждём чуть-чуть (на случай гонки с последним тиком
// цены), и если тейк так и не сработал — идём резолвить через Gamma API.
const CLOSE_FALLBACK_BUFFER_MS = 5 * 1000;

const RESOLVE_CHECK_DELAY_SEC = 180;
const RESOLVE_RETRY_MS = 30 * 1000;
const RESOLVE_GIVE_UP_MS = 30 * 60 * 1000;

const REDEEM_POLL_MS = 60 * 1000;
const GAMMA_HOST = "https://gamma-api.polymarket.com";

// ─── v5.2: честная просадка ───
// Сколько секунд ДО закрытия окна перестаём учитывать цену для
// просадки — в этот момент ликвидность почти всегда уже исчезла, и
// любая цена там — мусор, а не реальный рынок.
const DRAWDOWN_IGNORE_LAST_SEC = Number(process.env.FASTFLIP_DRAWDOWN_IGNORE_LAST_SEC ?? "10");

// ─── v5.2: цена BTC через Chainlink (Polygon) ───
// Адрес официального Chainlink BTC/USD price feed на Polygon mainnet.
// Проверено по PolygonScan и data.chain.link/polygon/mainnet/crypto-usd/btc-usd.
// Можно переопределить через env, если Chainlink сменит контракт.
const CHAINLINK_BTC_USD_FEED = process.env.CHAINLINK_BTC_USD_FEED ?? "0xc907E116054Ad103354f2D350FD2514433D57F6f";

// ─── Настройки, которые можно менять на лету через Telegram ───
const settings = {
  entryPrice: Number(process.env.FASTFLIP_ENTRY_PRICE ?? "0.98"),
  tpPrice: Number(process.env.FASTFLIP_TP_PRICE ?? "0.99"),
  entryWindowSec: Number(process.env.FASTFLIP_ENTRY_WINDOW_SEC ?? "30"),
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
  // v5.2: обновляется только по "честным" тикам, см. maybeTriggerExit.
  minPriceSinceEntry: number;
  // Таймер, который резолвит сделку через Gamma API, если тейк не
  // успел сработать до закрытия окна. Отменяем его, если тейк всё же
  // сработал раньше.
  resolveFallbackTimer: NodeJS.Timeout | null;
  // v5.2: цена BTC (Chainlink) на момент открытия окна и на момент
  // входа в сделку + % изменения между ними. null, если по какой-то
  // причине цену получить не удалось (тогда строка в отчёте не
  // добавляется).
  btcStartPrice: number | null;
  btcEntryPrice: number | null;
  btcChangePct: number | null;
}

// v5.2: снимок цены BTC на момент открытия конкретного 5-минутного окна.
interface BtcStartSnapshot {
  startPrice: number | null;
  closeTimeMs: number;
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

// ─── v5.2.2: получение цены BTC/USD через Chainlink price feed (Polygon) ───
// Без сторонних библиотек — обычный JSON-RPC eth_call. Специально
// НЕ завязано жёстко на один RPC_URL — если он временно недоступен или
// у провайдера истёк ключ, бот пробует по очереди СПИСОК бесплатных
// публичных Polygon RPC, пока один из них не ответит. Порядок:
//   1) FASTFLIP_CHAINLINK_RPC_URLS — список через запятую, если задан явно;
//   2) иначе FASTFLIP_CHAINLINK_RPC_URL / RPC_URL, если заданы, ПЛЮС
//      несколько известных бесплатных публичных нод как подстраховка.
const CHAINLINK_RPC_URLS: string[] = (() => {
  const explicit = process.env.FASTFLIP_CHAINLINK_RPC_URLS;
  if (explicit) {
    return explicit
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }

  const primary = process.env.FASTFLIP_CHAINLINK_RPC_URL ?? process.env.RPC_URL;
  const fallbacks = [
    "https://polygon-rpc.com",
    "https://rpc.ankr.com/polygon",
    "https://polygon.llamarpc.com",
    "https://polygon-bor-rpc.publicnode.com",
  ];

  const list = primary ? [primary, ...fallbacks] : fallbacks;
  // Убираем дубликаты, сохраняя порядок.
  return [...new Set(list)];
})();

let chainlinkDecimalsCache: number | null = null;

async function ethCall(rpcUrl: string, to: string, data: string): Promise<string> {
  const resp = await fetch(rpcUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "eth_call",
      params: [{ to, data }, "latest"],
    }),
  });

  // v5.2.1: если RPC вернул не-200 (провайдер режет запрос, лимит, требует
  // ключ в URL и т.п.) — показываем статус и тело ответа целиком, а не
  // падаем на непонятном "Unexpected token" из resp.json().
  if (!resp.ok) {
    const bodyText = await resp.text().catch(() => "<не удалось прочитать тело>");
    throw new Error(`RPC ответил ${resp.status} ${resp.statusText}: ${bodyText.slice(0, 300)}`);
  }

  const json = await resp.json();

  if (json.error) {
    // Разные провайдеры кладут ошибку по-разному: { error: { message } },
    // { error: "строка" }, иногда без message вообще — логируем весь
    // объект целиком, чтобы не терять реальную причину.
    const detail = typeof json.error === "string" ? json.error : JSON.stringify(json.error);
    throw new Error(`RPC вернул error: ${detail}`);
  }

  if (typeof json.result !== "string") {
    throw new Error(`RPC вернул неожиданный ответ без result: ${JSON.stringify(json).slice(0, 300)}`);
  }

  return json.result;
}

/** decimals() — сколько знаков после запятой зашито в ответах фида (обычно 8 для BTC/USD). Кэшируем — не меняется. */
async function getChainlinkDecimals(rpcUrl: string): Promise<number> {
  if (chainlinkDecimalsCache !== null) return chainlinkDecimalsCache;
  const result = await ethCall(rpcUrl, CHAINLINK_BTC_USD_FEED, "0x313ce567"); // selector decimals()
  chainlinkDecimalsCache = parseInt(result, 16);
  return chainlinkDecimalsCache;
}

/**
 * Текущая цена BTC/USD по Chainlink на Polygon. Перебирает
 * CHAINLINK_RPC_URLS по очереди, пока один из них не ответит успешно.
 * Возвращает null, только если ВСЕ эндпоинты из списка не сработали —
 * вызывающий код должен просто пропустить добавление % в отчёт в этом
 * случае, а не падать.
 */
async function getBtcPriceChainlink(): Promise<number | null> {
  for (const rpcUrl of CHAINLINK_RPC_URLS) {
    try {
      const [decimals, roundData] = await Promise.all([
        getChainlinkDecimals(rpcUrl),
        ethCall(rpcUrl, CHAINLINK_BTC_USD_FEED, "0xfeaf968c"), // selector latestRoundData()
      ]);

      // latestRoundData() возвращает 5 слов по 32 байта:
      // (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
      // Нас интересует только второе слово — answer.
      const hex = roundData.slice(2);
      const answerHex = hex.slice(64, 128);
      let answer = BigInt(`0x${answerHex}`);

      // На случай отрицательного значения (для цен фидов не встречается,
      // но на всякий случай корректно раскодируем two's complement).
      const MAX_INT256 = BigInt(2) ** BigInt(255);
      if (answer >= MAX_INT256) {
        answer -= BigInt(2) ** BigInt(256);
      }

      return Number(answer) / 10 ** decimals;
    } catch (err) {
      console.error(`[chainlink] эндпоинт ${rpcUrl} не сработал:`, err);
      // Пробуем следующий эндпоинт из списка.
    }
  }

  console.error("[chainlink] все RPC-эндпоинты для получения цены BTC не сработали — пропускаю % в этом отчёте.");
  return null;
}

/** Результат попытки рыночного (FOK) ордера. filledSize=0 значит "не исполнился". */
interface MarketOrderResult {
  orderId: string | null;
  filledSize: number;
  avgPrice: number;
}

class FastFlipMarketBot {
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

  // v5.2: цена BTC на момент открытия каждого 5-минутного окна,
  // ключ — eventSlug. Заполняется таймером в scheduleBtcStartSnapshot.
  private btcStartPrices = new Map<string, BtcStartSnapshot>();

  constructor(
    private clob: ClobService | null,
    private telegram: ReturnType<typeof createTelegramNotifier>,
  ) {}

  getStatus(): string {
    const watchedMarkets = this.tokenIndex.size / 2;
    return (
      `Режим: рыночные ордера (FOK), BTC, без выбора цели заранее\n` +
      `Цена входа: ${settings.entryPrice} | Тейк: ${settings.tpPrice} | Окно входа: последние ${settings.entryWindowSec}с\n` +
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
   * v5.2: планирует захват цены BTC ровно на момент открытия окна
   * (closeTimeMs - windowMinutes). Если окно уже открылось к моменту
   * обнаружения (например, бот только что перезапустился) — берём цену
   * немедленно, это лучше, чем ничего.
   */
  private scheduleBtcStartSnapshot(market: CryptoUpDownMarket): void {
    if (this.btcStartPrices.has(market.eventSlug)) return;

    const closeTimeMs = market.closeTimeMs;
    this.btcStartPrices.set(market.eventSlug, { startPrice: null, closeTimeMs });

    const openTimeMs = closeTimeMs - market.windowMinutes * 60 * 1000;
    const delay = Math.max(0, openTimeMs - Date.now());

    setTimeout(async () => {
      const price = await getBtcPriceChainlink();
      const snap = this.btcStartPrices.get(market.eventSlug);
      if (snap) snap.startPrice = price;
      console.log(
        `[btc-price] стартовая цена окна "${market.title}" (${market.eventSlug}): ${price !== null ? price.toFixed(2) : "н/д"}`,
      );
    }, delay);
  }

  /** v5.2: чистим снимки старых окон, чтобы Map не рос бесконечно. */
  private pruneBtcStartPrices(): void {
    const cutoff = Date.now() - HOUR_MS;
    for (const [slug, snap] of this.btcStartPrices) {
      if (snap.closeTimeMs < cutoff) this.btcStartPrices.delete(slug);
    }
  }

  /**
   * Пересканирует список активных BTC 5-минутных рынков и обновляет
   * подписку PriceWatcher. Пока открыта позиция или идёт попытка входа —
   * НЕ трогаем существующую подписку (там уже есть нужный токен, ему
   * ничего не мешает продолжать присылать тики).
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
        m.coin.toUpperCase() === COIN.toUpperCase() &&
        m.windowMinutes === TARGET_WINDOW_MINUTES &&
        m.closeTimeMs - now <= observeWindowMs(m.windowMinutes),
    );

    // v5.2: как только видим рынок — сразу планируем захват стартовой
    // цены BTC на момент открытия его окна (даже если позиция сейчас
    // не откроется на нём — снимок дешёвый и понадобится, если сетап
    // сложится именно здесь).
    for (const m of markets) {
      this.scheduleBtcStartSnapshot(m);
    }
    this.pruneBtcStartPrices();

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
    // отчёте, только по "честным" тикам — см. maybeTriggerExit) и
    // проверяем не пора ли выходить по тейку ──
    if (this.openPosition) {
      this.maybeTriggerExit(this.openPosition, market, price, update);
      return;
    }

    // ── Ищем вход: сетап может сложиться на ЛЮБОМ из отслеживаемых
    // сейчас рынков, мы не привязаны к одной заранее выбранной цели ──
    if (this.attemptInProgress) return;

    this.checkHourlyReset();

    if (this.tradesThisHour >= settings.quotaPerHour) return; // квота часа выполнена

    const secToClose = (market.closeTimeMs - Date.now()) / 1000;

    // Вход разрешён ТОЛЬКО в последние entryWindowSec секунд перед
    // закрытием ИМЕННО ЭТОЙ пятиминутки.
    if (secToClose > settings.entryWindowSec || secToClose < 0) return;

    if (price < settings.entryPrice) return;

    this.attemptInProgress = true;
    const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
    this.executeMarketEntry(market, side, tokenId, price, secToClose);
  }

  private maybeTriggerExit(pos: OpenPosition, market: CryptoUpDownMarket, price: number, update: PriceUpdate): void {
    // Тик пришёл не по тому рынку, где у нас открыта позиция — игнор.
    if (market.eventSlug !== pos.market.eventSlug) return;

    // ── v5.2: честное обновление минимума для просадки ──
    // Обновляем minPriceSinceEntry ТОЛЬКО если:
    //   а) в стакане одновременно есть и бид, и аск (рынок живой,
    //      двусторонний, а не пустой хвост из одной мусорной заявки), И
    //   б) до закрытия окна ещё есть время (не последние
    //      DRAWDOWN_IGNORE_LAST_SEC секунд, когда ликвидность почти
    //      гарантированно исчезает).
    const bothSidesPresent = update.bestBid !== null && update.bestAsk !== null;
    const secToClose = (market.closeTimeMs - Date.now()) / 1000;
    const withinIgnoreWindow = secToClose <= DRAWDOWN_IGNORE_LAST_SEC;

    if (bothSidesPresent && !withinIgnoreWindow && price < pos.minPriceSinceEntry) {
      pos.minPriceSinceEntry = price;
    }

    if (pos.closed || pos.exitAttemptInProgress) return;

    // Реальная цель выхода: не просто "цена дошла до номинального
    // тейка", а "цена дошла до тейка И это даёт реальную прибыль сверх
    // того, что мы заплатили при покупке". Если проскальзывание на
    // входе уже съело весь запас (купили по цене ≥ тейка) — ждём
    // резолва вместо бессмысленного выхода в ноль/убыток.
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
    nominalPrice: number; // 0.98 при входе / 0.99 при выходе — используется методом только как fallback, если стакан вдруг пуст
  }): Promise<MarketOrderResult> {
    if (DRY_RUN || !this.clob) {
      // В DRY_RUN считаем, что рыночный ордер исполнился мгновенно и
      // целиком по запрошенной (номинальной) цене — без реального похода
      // на биржу.
      return { orderId: null, filledSize: params.size, avgPrice: params.nominalPrice };
    }

    try {
      // Это тот самый "market-по-факту" метод — несмотря на название
      // placeLimitOrder, он берёт свежий стакан ПРЯМО СЕЙЧАС и целится в
      // реальную лучшую цену (+буфер MARKET_ORDER_SLIPPAGE_PCT), с жёстким
      // потолком/полом 0.999/0.001. Это FAK — исполняется сразу, что
      // может, остальное снимается, ничего не висит в стакане.
      const resp = await this.clob.placeLimitOrder({
        tokenId: params.tokenId,
        side: params.side === "BUY" ? Side.BUY : Side.SELL,
        price: params.nominalPrice,
        size: params.size,
        maxSlippagePct: MARKET_ORDER_SLIPPAGE_PCT,
      });

      // ⚠️ making/taking семантика биржи — "сколько ОТДАЛИ" / "сколько
      // ПОЛУЧИЛИ". При ПОКУПКЕ ты отдаёшь доллары и получаешь акции — то
      // есть для BUY поле resp.filledSize (=makingAmount) на самом деле
      // содержит ДОЛЛАРЫ, а resp.filledUsdc (=takingAmount) содержит
      // АКЦИИ — противоположно тому, что предполагают их названия. Для
      // ПРОДАЖИ всё совпадает с названиями полей правильно, поэтому
      // меняем местами только для BUY.
      const rawA = Number(resp.filledSize ?? 0);
      const rawB = Number(resp.filledUsdc ?? 0);

      const filledShares = params.side === "BUY" ? rawB : rawA;
      const filledDollars = params.side === "BUY" ? rawA : rawB;

      // Средняя цена исполнения = сколько USDC ушло / сколько акций закрылось.
      const avgPrice = filledShares > 0 ? filledDollars / filledShares : 0;

      return { orderId: null, filledSize: filledShares, avgPrice };
    } catch (err) {
      // placeLimitOrder кидает исключение, если ордер не исполнился
      // (нет встречной ликвидности для FAK) или был отклонён — это
      // ожидаемая ситуация, не баг, просто считаем как "не вошли".
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
        `   Сторона: ${side} | Цена сейчас: ~${priceAtEntry} | До закрытия: ${secToClose.toFixed(1)}с\n` +
        `   Покупаем: ${size.toFixed(2)} акций рыночным ордером (цель ~${settings.entryPrice}, ~$${TRADE_SIZE_USD})`,
    );

    // v5.2: запрашиваем цену BTC на момент входа ПАРАЛЛЕЛЬНО с ордером,
    // а не до него — чтобы не замедлять само исполнение сделки ни на
    // миллисекунду. Если Chainlink не ответит вовремя или упадёт —
    // просто не добавим строку в отчёт, сделка на это не влияет.
    const [result, btcEntryPrice] = await Promise.all([
      this.placeMarketOrder({ tokenId, side: "BUY", size, nominalPrice: settings.entryPrice }),
      getBtcPriceChainlink(),
    ]);

    if (result.filledSize <= 0) {
      console.log(`   ⏳ Рыночная покупка не исполнилась (eventSlug: ${market.eventSlug}). Продолжаю мониторинг.`);
      this.attemptInProgress = false;
      return;
    }

    console.log(`   💰 ПОКУПКА ИСПОЛНЕНА ПО РЫНКУ: ${result.filledSize.toFixed(2)} акций по ~${result.avgPrice}.`);

    // v5.2: считаем % изменения цены BTC от начала окна до входа.
    const btcStartSnapshot = this.btcStartPrices.get(market.eventSlug);
    const btcStartPrice = btcStartSnapshot?.startPrice ?? null;
    let btcChangePct: number | null = null;
    if (btcStartPrice !== null && btcEntryPrice !== null && btcStartPrice > 0) {
      btcChangePct = ((btcEntryPrice - btcStartPrice) / btcStartPrice) * 100;
    }

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
      btcStartPrice,
      btcEntryPrice,
      btcChangePct,
    };
    this.openPosition = pos;

    // ── Проверка на аномальное исполнение: купили НАМНОГО ниже, чем
    // ожидали — значит цена рухнула прямо в момент исполнения ордера.
    // Это больше не "почти гарантированная" позиция, а фактически
    // монетка. Сразу пробуем аварийно выйти, не дожидаясь резолва.
    const isBadFill = pos.buyPrice < settings.entryPrice - BAD_FILL_TOLERANCE;

    if (isBadFill) {
      if (this.telegram) {
        await this.telegram.send(
          `🚨 Цена рухнула ПРЯМО во время исполнения ордера: купили по ${result.avgPrice.toFixed(3)} вместо ожидаемых ~${settings.entryPrice} — рынок начал резкий разворот. Это больше не почти-гарантированная позиция. Пробую аварийно продать обратно прямо сейчас.`,
        );
      }

      this.emergencyExit(pos);
      return;
    }

    // Если проскальзывание на входе уже съело весь запас прибыли —
    // предупреждаем прямо сейчас, чтобы было видно в моменте, а не
    // только постфактум по цифрам в истории Polymarket.
    const effectiveExitPrice = Math.max(settings.tpPrice, pos.buyPrice + MIN_PROFIT_MARGIN);
    const slippageAteMargin = effectiveExitPrice > settings.tpPrice + 0.0001;

    if (this.telegram) {
      const warning = slippageAteMargin
        ? `\n⚠️ Проскальзывание съело запас прибыли (купили по ${result.avgPrice.toFixed(3)}, номинальный тейк ${settings.tpPrice}) — мгновенный выход отменён, жду реального роста цены или резолва.`
        : `\nЖду тейк ${settings.tpPrice} по рынку, либо закрытия окна...`;

      const btcLine =
        btcChangePct !== null
          ? `\nBTC от начала окна до входа: ${btcChangePct >= 0 ? "+" : ""}${btcChangePct.toFixed(2)}%`
          : "";

      await this.telegram.send(
        `💰 Куплено по рынку: ${market.title}\nСторона: ${side}\nЦена: ${result.avgPrice.toFixed(3)} | Размер: ${result.filledSize.toFixed(2)}${btcLine}${warning}`,
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

  /**
   * Аварийный выход — вызывается, когда фактическая цена покупки
   * оказалась намного ниже ожидаемой (см. BAD_FILL_TOLERANCE). Пробует
   * СРАЗУ продать обратно по рынку, что бы ни было в стакане, вместо
   * того чтобы слепо держать до резолва позицию, которая перестала
   * быть "почти гарантированной". Если продать сразу не получилось —
   * всё равно ставим обычный резервный таймер на резолв, чтобы сделка
   * не осталась висеть без исхода.
   */
  private async emergencyExit(pos: OpenPosition): Promise<void> {
    console.log(
      `\n🚨 АВАРИЙНЫЙ ВЫХОД: [BTC / 5мин] "${pos.market.title}" — цена рухнула во время исполнения, пробую продать немедленно.`,
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
      await this.notifyClose(pos, "аварийный выход (цена рухнула при исполнении)", outcome, profit);
      this.finishTrade(pos);
      return;
    }

    console.log(`   ⚠️ Аварийная продажа не прошла — держим до резолва как обычно.`);

    // Не получилось продать сразу — ставим обычный резервный таймер на
    // резолв, как в штатном сценарии.
    const msUntilCloseCheck = Math.max(0, pos.market.closeTimeMs - Date.now() + CLOSE_FALLBACK_BUFFER_MS);
    pos.resolveFallbackTimer = setTimeout(() => {
      if (pos.closed) return;
      console.log(`   ⏳ Аварийный выход не удался — жду официальный резолв (eventSlug: ${pos.market.eventSlug}).`);
      this.scheduleResolveFallback(pos);
    }, msUntilCloseCheck);
  }

  private async executeMarketExit(pos: OpenPosition, priceAtExit: number): Promise<void> {
    console.log(
      `\n🎯 ВЫХОД ПО РЫНКУ: [BTC / 5мин] "${pos.market.title}"\n` +
        `   Сторона: ${pos.side} | Цена сейчас: ~${priceAtExit} | Продаём: ${pos.filledSize.toFixed(2)} акций рыночным ордером (цель ~${settings.tpPrice})`,
    );

    const result = await this.placeMarketOrder({
      tokenId: pos.tokenId,
      side: "SELL",
      size: pos.filledSize,
      nominalPrice: settings.tpPrice,
    });

    if (result.filledSize < pos.filledSize - 0.001) {
      // Не удалось продать целиком по рынку (цена успела уйти обратно
      // ниже тейка раньше, чем ордер долетел). Оставляем позицию
      // открытой — попробуем снова на следующем тике цены, либо сработает
      // fallback-резолв при закрытии окна.
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
    await this.notifyClose(pos, "тейк по рынку", "WIN", profit);
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

  /** Вызывается после любого способа закрытия сделки — обнуляет позицию, засчитывает в квоту часа и возвращается к мониторингу всех активных рынков. */
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

    // Сразу пересканируем рынки — не ждём следующего тика таймера.
    this.refreshMarkets().catch((err) => console.error("[refresh] ошибка:", (err as Error).message));
  }

  private async notifyClose(
    pos: OpenPosition,
    how: string,
    outcome: "WIN" | "LOSS",
    profit: number,
  ): Promise<void> {
    const sign = outcome === "WIN" ? "✅ ПРИБЫЛЬ" : "🔻 УБЫТОК";

    // Максимальная просадка сделки (v5.2: считается только по честным
    // двусторонним тикам вне последних DRAWDOWN_IGNORE_LAST_SEC секунд
    // окна — см. maybeTriggerExit).
    const drawdown = Math.max(0, pos.buyPrice - pos.minPriceSinceEntry);
    const drawdownPct = pos.buyPrice > 0 ? (drawdown / pos.buyPrice) * 100 : 0;

    const btcLine =
      pos.btcChangePct !== null
        ? `\nВо время входа цена BTC от начала окна была ${pos.btcChangePct >= 0 ? "+" : ""}${pos.btcChangePct.toFixed(2)}%`
        : "";

    const msg =
      `${sign} (${outcome})\n` +
      `Способ закрытия: ${how}\n` +
      `${pos.market.title}\n` +
      `Сторона: ${pos.side}\n` +
      `Профит: ${profit >= 0 ? "+" : ""}$${profit.toFixed(3)}\n` +
      `Цена покупки: ${pos.buyPrice.toFixed(3)} | Мин. цена за сделку: ${pos.minPriceSinceEntry.toFixed(3)}\n` +
      `Максимальная просадка: ${drawdown.toFixed(3)} (${drawdownPct.toFixed(1)}%)` +
      btcLine;

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
          await telegram?.send(`Цена входа установлена: ${settings.entryPrice}`);
          continue;
        }

        const tpMatch = text.match(/^тейк\s+([\d.]+)$/);
        if (tpMatch) {
          settings.tpPrice = Number(tpMatch[1]);
          await telegram?.send(`Тейк-профит установлен: ${settings.tpPrice}`);
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
    `Актив: BTC only | Вход: ${settings.entryPrice} (рынком, окно ${settings.entryWindowSec}с) | ` +
      `Тейк: ${settings.tpPrice} (рынком) | Квота: ${settings.quotaPerHour}/час | Стоп: убран`,
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
    console.log("Telegram-команды включены: цена X / тейк X / окно X / квота X / статус");
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