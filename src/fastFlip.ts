/**
 * "Быстрый флип" v5.3 — РЫНОЧНЫЕ ордера, вход только в последние секунды
 * перед закрытием, С КОРИДОРОМ ВХОДА (нижняя+верхняя граница цены), С
 * КВОТОЙ сделок в час, С СТОП-ЛОССОМ.
 * ПАРАЛЛЕЛЬНЫЙ старым sniperTrader.ts / fastFlip.ts / fastFlipMarket.ts
 * (их не трогаем, не запускаем).
 *
 * Изменения относительно v5.2 (по запросу):
 *
 *   1. entryPrice по умолчанию 0.97 (было 0.98).
 *
 *   2. НОВОЕ: maxEntryPrice (по умолчанию 0.98) — верхняя граница входа.
 *      Раньше условие входа было только "price >= entryPrice", БЕЗ
 *      потолка — то есть бот теоретически мог купить и по 0.995, если
 *      цена именно в этот тик перескочила сразу высоко. Теперь вход
 *      разрешён ТОЛЬКО когда цена находится В КОРИДОРЕ
 *      [entryPrice, maxEntryPrice], т.е. по умолчанию 0.97–0.98.
 *      Если цена уже выше maxEntryPrice — сетап считается "проехавшим",
 *      бот НЕ покупает и ждёт следующую возможность (следующий тик /
 *      следующий рынок).
 *
 *   3. entryWindowSec по умолчанию 60 (было 30) — вход рассматривается
 *      только в последние 60 секунд до закрытия текущей 5-минутки.
 *
 *   4. tpPrice остаётся 0.99 по умолчанию — без изменений.
 *
 *   5. Скорость выхода: НЕ МЕНЯЛАСЬ, потому что уже была максимальной.
 *      Проверка тейка/стопа идёт на КАЖДОМ обновлении цены из
 *      PriceWatcher (событийно, без polling и без искусственных пауз).
 *      Как только цена достигает эффективного тейка — в ту же
 *      миллисекунду улетает рыночный (FAK) SELL-ордер через
 *      ClobService.placeLimitOrder (см. комментарий там же — по факту
 *      это market-ордер, метод берёт свежий стакан и бьёт в лучшую
 *      цену). Единственное "ожидание" в системе — это резервный
 *      механизм resolve через Gamma API, который включается ТОЛЬКО
 *      если ни тейк, ни стоп не сработали до самого закрытия окна.
 *      Соответственно никакого отдельного "ускорения продажи" делать
 *      не нужно было — логика и так продаёт при первой же возможности.
 *
 * КАК ЭТО РАБОТАЕТ (не изменилось с v5):
 *
 *  Бот НЕ выбирает заранее, в какую пятиминутку он войдёт, и НЕ бросает
 *  кубик в начале часа. Вместо этого он ОДНОВРЕМЕННО смотрит на ВСЕ
 *  сейчас активные BTC 5-минутные Up/Down рынки. Каждую секунду через
 *  все эти рынки летят обновления цен, и на КАЖДОЕ обновление бот
 *  проверяет: "а не сложился ли для этого конкретного токена наш
 *  сетап прямо сейчас?"
 *
 *  Сетап — это:
 *    1. Цена в коридоре [entryPrice, maxEntryPrice] (по умолчанию
 *       0.97–0.98), И
 *    2. До закрытия ИМЕННО ЭТОЙ пятиминутки осталось не больше
 *       entryWindowSec секунд (60 по умолчанию), И
 *    3. Квота сделок на этот час ещё не исчерпана.
 *
 *  ВХОД И ВЫХОД — РЫНОЧНЫМИ ОРДЕРАМИ через `ClobService.placeLimitOrder`
 *  (несмотря на название метода — по факту это FAK market-ордер):
 *    - Метод сам берёт СВЕЖИЙ стакан прямо в момент вызова и целится в
 *      реальную лучшую цену продажи/покупки (+небольшой буфер).
 *    - Жёсткий потолок/пол 0.999 / 0.001.
 *    - FAK (fill-and-kill): исполняется сразу, что может, остальное
 *      снимается — ничего не висит в стакане.
 *
 *  Выход из позиции возможен ТРЕМЯ способами:
 *    - ТЕЙК: цена дошла до settings.tpPrice (с поправкой на реальную
 *      цену покупки, см. MIN_PROFIT_MARGIN) — рыночным ордером,
 *      МГНОВЕННО на этом же тике.
 *    - СТОП: цена упала до settings.slPrice или ниже — рыночным ордером,
 *      немедленно, независимо от того, сколько времени осталось до
 *      закрытия окна.
 *    - РЕЗОЛВ: если ни тейк, ни стоп не сработали до закрытия окна —
 *      ждём официальный резолв через Gamma API.
 *
 * ✅ ПОДТВЕРЖДЕНО НА ЖИВОЙ СДЕЛКЕ: making/taking семантика биржи — это
 *   "сколько ОТДАЛИ" / "сколько ПОЛУЧИЛИ". При ПОКУПКЕ поля
 *   resp.filledSize/resp.filledUsdc содержат доллары/акции В ОБРАТНОМ
 *   порядке относительно своих названий. Для ПРОДАЖИ порядок совпадает
 *   с названиями полей. Код ниже (placeMarketOrder) меняет их местами
 *   именно для BUY.
 *
 * НАСТРОЙКИ НА ЛЕТУ ЧЕРЕЗ TELEGRAM (без передеплоя):
 *   цена 0.97       — нижняя цена входа
 *   цена_макс 0.98  — верхняя цена входа (потолок коридора)
 *   тейк 0.99       — цена выхода в плюс
 *   стоп 0.90       — цена аварийного выхода в минус (стоп-лосс)
 *   окно 60         — сколько секунд до закрытия окна разрешён вход
 *   квота 3         — сколько сделок максимум за текущий час
 *   статус          — текущие настройки + что происходит сейчас
 *
 * DRY_RUN=true по умолчанию (FASTFLIP_DRY_RUN=false для реальных денег).
 * В DRY_RUN бот полностью симулирует сделку (вход → ожидание тейка,
 * стопа или резолва → выход), чтобы логику можно было проверить перед
 * реальными деньгами.
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

// ВАЖНО: пересоздание PriceWatcher (refreshMarkets) полностью рвёт
// текущее WebSocket-соединение и поднимает новое — на время
// переподключения бот НЕ ВИДИТ никаких обновлений цены вообще. Если
// это происходит ровно в момент, когда цена подходит к entryPrice —
// вход будет пропущен молча, без единой ошибки в логах.
//
// Поэтому: если хотя бы один из уже отслеживаемых рынков находится
// ближе, чем entryWindowSec + этот буфер, к своему закрытию — вообще
// НЕ трогаем вотчер в этом цикле рефреша, даже если список токенов
// формально поменялся. Лучше пропустить один цикл дискавери новых
// рынков, чем оборвать соединение прямо перед потенциальным входом.
const REFRESH_SAFETY_BUFFER_SEC = 30;

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
// резкий разворот в тот же момент). В этом случае сразу пробуем
// аварийно продать обратно вместо того, чтобы слепо держать до резолва
// — см. executeMarketEntry/emergencyExit.
const BAD_FILL_TOLERANCE = Number(process.env.FASTFLIP_BAD_FILL_TOLERANCE ?? "0.02");

// После закрытия окна ждём чуть-чуть (на случай гонки с последним тиком
// цены), и если ни тейк, ни стоп так и не сработали — идём резолвить
// через Gamma API.
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
  // считается "проехавшим", вход не совершается. Это защита от того,
  // чтобы купить слишком дорого, если цена одним тиком перескочила
  // высоко (раньше такого потолка не было вообще).
  maxEntryPrice: Number(process.env.FASTFLIP_MAX_ENTRY_PRICE ?? "0.98"),
  tpPrice: Number(process.env.FASTFLIP_TP_PRICE ?? "0.99"),
  // Стоп-лосс: если цена после входа падает ДО этого уровня ИЛИ НИЖЕ —
  // бот немедленно пытается выйти по рынку, не дожидаясь тейка или
  // резолва. Поставь 0, чтобы фактически отключить стоп.
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
  // Таймер, который резолвит сделку через Gamma API, если ни тейк, ни
  // стоп не успели сработать до закрытия окна. Отменяем его, если
  // выход всё же сработал раньше.
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

/** Официальный резолв рынка через Gamma API (fallback-кейс, когда ни тейк, ни стоп не сработали по рынку). */
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

  constructor(
    private clob: ClobService | null,
    private telegram: ReturnType<typeof createTelegramNotifier>,
  ) {}

  getStatus(): string {
    const watchedMarkets = this.tokenIndex.size / 2;
    return (
      `Режим: рыночные ордера (FOK), BTC, коридор входа\n` +
      `Вход: ${settings.entryPrice}–${settings.maxEntryPrice} | Тейк: ${settings.tpPrice} | Стоп: ${settings.slPrice} | Окно входа: последние ${settings.entryWindowSec}с\n` +
      `Квота: ${this.tradesThisHour}/${settings.quotaPerHour} сделок в этом часе\n` +
      `Сейчас отслеживается активных 5-мин рынков: ${watchedMarkets}\n` +
      `Всего сделок с запуска: ${this.tradesTotal}\n` +
      `Открытая позиция: ${this.openPosition ? `${this.openPosition.market.title} (${this.openPosition.side}), цена входа ${this.openPosition.buyPrice.toFixed(3)}` : "нет"}`
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
   * true, если хоть один из СЕЙЧАС отслеживаемых рынков уже настолько
   * близко к закрытию, что в любую секунду может сложиться сетап входа
   * (или уже складывается). Используется, чтобы НЕ пересоздавать
   * WebSocket-подписку в этот момент — см. REFRESH_SAFETY_BUFFER_SEC.
   */
  private hasCriticalMarket(): boolean {
    const now = Date.now();
    const criticalMs = (settings.entryWindowSec + REFRESH_SAFETY_BUFFER_SEC) * 1000;
    for (const info of this.tokenIndex.values()) {
      const msToClose = info.market.closeTimeMs - now;
      if (msToClose >= 0 && msToClose <= criticalMs) return true;
    }
    return false;
  }

  /**
   * Пересканирует список активных BTC 5-минутных рынков и обновляет
   * подписку PriceWatcher. Пока открыта позиция, идёт попытка входа,
   * ИЛИ любой из уже отслеживаемых рынков в критическом окне (см.
   * hasCriticalMarket) — НЕ трогаем существующую подписку вообще,
   * чтобы не оборвать соединение в момент, когда цена может как раз
   * пересекать коридор входа.
   */
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
    // отчёте) и проверяем не пора ли выходить по стопу или по тейку ──
    if (this.openPosition) {
      this.maybeTriggerExit(this.openPosition, market, price);
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

    // Вход разрешён ТОЛЬКО если цена внутри коридора [entryPrice,
    // maxEntryPrice]. Ниже коридора — сетап ещё не сложился, ждём.
    // Выше коридора — цена уже "проехала" точку входа, слишком дорого
    // покупать, пропускаем этот сетап (не гонимся за ценой).
    if (price < settings.entryPrice) return;
    if (price > settings.maxEntryPrice) return;

    this.attemptInProgress = true;
    const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
    this.executeMarketEntry(market, side, tokenId, price, secToClose);
  }

  /**
   * Вызывается на каждый тик цены, пока позиция открыта. Порядок
   * проверок важен: СТОП проверяется ПЕРВЫМ, до тейка.
   */
  private maybeTriggerExit(pos: OpenPosition, market: CryptoUpDownMarket, price: number): void {
    // Тик пришёл не по тому рынку, где у нас открыта позиция — игнор.
    if (market.eventSlug !== pos.market.eventSlug) return;

    // Обновляем минимальную цену за время жизни позиции — независимо от
    // того, сработает ли выход сейчас. Это и есть просадка сделки.
    if (price < pos.minPriceSinceEntry) {
      pos.minPriceSinceEntry = price;
    }

    if (pos.closed || pos.exitAttemptInProgress) return;

    // ── СТОП-ЛОСС: цена пересекла границу вниз — выходим НЕМЕДЛЕННО по
    // рынку, что бы ни было с тейком и сколько бы времени ни оставалось
    // до закрытия окна. Условие "<=", чтобы сработать и точно на уровне.
    if (settings.slPrice > 0 && price <= settings.slPrice) {
      pos.exitAttemptInProgress = true;
      this.executeStopLoss(pos, price);
      return;
    }

    // Реальная цель выхода в плюс: не просто "цена дошла до номинального
    // тейка", а "цена дошла до тейка И это даёт реальную прибыль сверх
    // того, что мы заплатили при покупке".
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
      `\n⚡ ВХОД ПО РЫНКУ: [BTC / 5мин] "${market.title}"\n` +
        `   Сторона: ${side} | Цена сейчас: ~${priceAtEntry} (коридор ${settings.entryPrice}-${settings.maxEntryPrice}) | До закрытия: ${secToClose.toFixed(1)}с\n` +
        `   Покупаем: ${size.toFixed(2)} акций рыночным ордером (~$${TRADE_SIZE_USD})`,
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
      minPriceSinceEntry: result.avgPrice,
      resolveFallbackTimer: null,
    };
    this.openPosition = pos;

    // ── Проверка на аномальное исполнение: купили НАМНОГО ниже, чем
    // ожидали — сразу пробуем аварийно выйти, не дожидаясь резолва.
    const isBadFill = pos.buyPrice < settings.entryPrice - BAD_FILL_TOLERANCE;

    if (isBadFill) {
      if (this.telegram) {
        await this.telegram.send(
          `🚨 Цена рухнула ПРЯМО во время исполнения ордера: купили по ${result.avgPrice.toFixed(3)} вместо ожидаемых ~${settings.entryPrice} — рынок начал резкий разворот. Пробую аварийно продать обратно прямо сейчас.`,
        );
      }

      this.emergencyExit(pos);
      return;
    }

    if (settings.slPrice > 0 && pos.buyPrice <= settings.slPrice) {
      console.log(
        `   ⚠️ Цена покупки (${pos.buyPrice.toFixed(3)}) уже на уровне стопа (${settings.slPrice}) или ниже — выход сработает на следующем тике.`,
      );
    }

    const effectiveExitPrice = Math.max(settings.tpPrice, pos.buyPrice + MIN_PROFIT_MARGIN);
    const slippageAteMargin = effectiveExitPrice > settings.tpPrice + 0.0001;

    if (this.telegram) {
      const warning = slippageAteMargin
        ? `\n⚠️ Проскальзывание съело запас прибыли (купили по ${result.avgPrice.toFixed(3)}, номинальный тейк ${settings.tpPrice}) — мгновенный выход отменён, жду реального роста цены, стопа или резолва.`
        : `\nЖду тейк ${settings.tpPrice} или стоп ${settings.slPrice} по рынку, либо закрытия окна...`;

      await this.telegram.send(
        `💰 Куплено по рынку: ${market.title}\nСторона: ${side}\nЦена: ${result.avgPrice.toFixed(3)} | Размер: ${result.filledSize.toFixed(2)}${warning}`,
      );
    }

    const msUntilCloseCheck = Math.max(0, market.closeTimeMs - Date.now() + CLOSE_FALLBACK_BUFFER_MS);
    pos.resolveFallbackTimer = setTimeout(() => {
      if (pos.closed) return;
      console.log(`   ⏳ Ни тейк, ни стоп не сработали до закрытия — жду официальный резолв (eventSlug: ${market.eventSlug}).`);
      this.scheduleResolveFallback(pos);
    }, msUntilCloseCheck);
  }

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

    console.log(`   ⚠️ Аварийная продажа не прошла — держим до стопа/резолва как обычно.`);

    const msUntilCloseCheck = Math.max(0, pos.market.closeTimeMs - Date.now() + CLOSE_FALLBACK_BUFFER_MS);
    pos.resolveFallbackTimer = setTimeout(() => {
      if (pos.closed) return;
      console.log(`   ⏳ Аварийный выход не удался — жду официальный резолв (eventSlug: ${pos.market.eventSlug}).`);
      this.scheduleResolveFallback(pos);
    }, msUntilCloseCheck);
  }

  /** Обычный выход в плюс — цена дошла до тейка (с поправкой на реальную цену покупки). Срабатывает мгновенно на том же тике, где цена достигла цели. */
  private async executeMarketExit(pos: OpenPosition, priceAtExit: number): Promise<void> {
    console.log(
      `\n🎯 ВЫХОД ПО РЫНКУ (ТЕЙК): [BTC / 5мин] "${pos.market.title}"\n` +
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
        `   ⚠️ Рыночная продажа не прошла целиком (${result.filledSize.toFixed(2)}/${pos.filledSize.toFixed(2)}). Пробую снова при следующем тике.`,
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
      `Цена покупки: ${pos.buyPrice.toFixed(3)} | Мин. цена за сделку: ${pos.minPriceSinceEntry.toFixed(3)}\n` +
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

        // "цена 0.97" — нижняя граница коридора входа.
        const priceMatch = text.match(/^цена\s+([\d.]+)$/);
        if (priceMatch) {
          settings.entryPrice = Number(priceMatch[1]);
          await telegram?.send(`Нижняя цена входа установлена: ${settings.entryPrice}`);
          continue;
        }

        // "цена_макс 0.98" — верхняя граница коридора входа.
        const maxPriceMatch = text.match(/^цена_макс\s+([\d.]+)$/);
        if (maxPriceMatch) {
          settings.maxEntryPrice = Number(maxPriceMatch[1]);
          await telegram?.send(`Верхняя цена входа установлена: ${settings.maxEntryPrice}`);
          continue;
        }

        const tpMatch = text.match(/^тейк\s+([\d.]+)$/);
        if (tpMatch) {
          settings.tpPrice = Number(tpMatch[1]);
          await telegram?.send(`Тейк-профит установлен: ${settings.tpPrice}`);
          continue;
        }

        const slMatch = text.match(/^стоп\s+([\d.]+)$/);
        if (slMatch) {
          settings.slPrice = Number(slMatch[1]);
          await telegram?.send(
            `Стоп-лосс установлен: ${settings.slPrice} (0 = выключен). При падении цены до этого уровня или ниже — немедленный выход по рынку.`,
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
      `Тейк: ${settings.tpPrice} (рынком) | Стоп: ${settings.slPrice} (рынком) | Квота: ${settings.quotaPerHour}/час`,
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
    console.log("Telegram-команды включены: цена X / цена_макс X / тейк X / стоп X / окно X / квота X / статус");
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