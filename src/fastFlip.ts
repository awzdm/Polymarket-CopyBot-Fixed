/**
 * "Быстрый флип" v5.6 — РЫНОЧНЫЕ ордера на ВХОД, НАСТОЯЩИЙ ВИСЯЩИЙ
 * ЛИМИТНИК (GTC) на ВЫХОД в плюс, СТОП-ЛОСС по рынку, ЖЁСТКИЙ ПОТОЛОК
 * ЦЕНЫ ПОКУПКИ (не выше maxEntryPrice ни при каких раскладах).
 *
 * ИЗМЕНЕНИЯ В v5.6 (относительно v5.5) — ПОЧЕМУ БОТ "ПОКУПАЛ ПО 98 И
 * ТУТ ЖЕ ПРОДАВАЛ ПО 97-98":
 *
 *   БАГ №1 (главная причина). Вход рыночным ордером ВСЕГДА целился в
 *   settings.entryPrice (фиксированные 0.97), а НЕ в реальную цену
 *   тика в момент входа (priceAtEntry), хотя она уже была известна.
 *   Метод placeLimitOrder (по факту FAK market-ордер) считает базу
 *   допустимого проскальзывания (maxSlippagePct) от переданной сюда
 *   nominalPrice. Если реальная цена тика была, скажем, 0.978, а бот
 *   всё равно целился от 0.97 — ордер либо не добирал до реального
 *   аска, либо (в тонком стакане перед самым закрытием окна) "проезжал"
 *   на несколько уровней глубже, чем должен был. Из-за этого реальная
 *   цена исполнения иногда вылезала за maxEntryPrice (0.98) — и тут же
 *   срабатывала защита isBadFillHigh, которая МГНОВЕННО (через секунду
 *   после покупки) аварийно продаёт позицию обратно по рынку — отсюда
 *   и "купил по 98, продал по 97-98" почти сразу.
 *
 *   ИСПРАВЛЕНО: теперь nominalPrice для входа = реальная priceAtEntry
 *   (последний известный тик), а не статичный settings.entryPrice.
 *
 *   БАГ №2 (усугублял №1). MAX_ENTRY_OVERSHOOT_TOLERANCE был всего
 *   0.002 (0.2 цента) — крайне жёсткий допуск для рыночного входа в
 *   последние секунды перед закрытием окна, когда стакан часто тонкий
 *   и цена естественно скачет на пару тиков при исполнении.
 *
 *   ИСПРАВЛЕНО: допуск увеличен до 0.008 по умолчанию (настраивается
 *   через FASTFLIP_MAX_ENTRY_OVERSHOOT_TOLERANCE), чтобы не путать
 *   нормальное проскальзывание тонкого рынка с реальной поломкой.
 *
 *   БАГ №3 (диагностика). Раньше сырые числа биржи (filledSize/
 *   filledUsdc) логировались ТОЛЬКО если filledSize > 0. Если фактическая
 *   цена покупки уходила за потолок, но исполнение всё же было — лог
 *   печатался, и это можно было увидеть, но НЕ печаталась сама причина
 *   срабатывания isBadFillHigh/isBadFillLow с точными числами.
 *
 *   ИСПРАВЛЕНО: сообщение о "плохом филле" теперь всегда содержит
 *   непокруглённую (полную) цену покупки и разницу с порогом — это
 *   видно и в консоли, и в Telegram, без необходимости лезть в сырые
 *   логи биржи.
 *
 * ВСЁ ОСТАЛЬНОЕ — БЕЗ ИЗМЕНЕНИЙ ОТНОСИТЕЛЬНО v5.5:
 *
 *   1. ВЫХОД В ПЛЮС — НАСТОЯЩИЙ висящий лимитный (GTC) ордер, выставляется
 *      сразу после исполнения покупки через ClobService.placeGtcLimitOrder,
 *      дальше исполняется сам на бирже, без ожидания тика.
 *   2. Если выставить лимитник с первого раза не получилось — ретраи,
 *      пока не будет ПРИНЯТ биржей.
 *   3. Жёсткий потолок покупки — проверка ПОСЛЕ реального исполнения
 *      (не только по цене тика до отправки), см. isBadFillHigh выше.
 *   4. Сверка перед стопом / перед уходом на резолв: бот проверяет
 *      статус висящего лимитника через getOrder() перед тем, как
 *      считать позицию всё ещё открытой.
 *
 * СТОП-ЛОСС остаётся рыночным (FAK), проверка "price <= settings.slPrice"
 * — это АБСОЛЮТНАЯ цена токена (0.0–1.0), а не доля от цены входа.
 * По умолчанию 0.90. Если стоп стоит близко к цене входа (например 0.5,
 * а вход в районе 0.97-0.98) — он вообще не должен сработать в обычных
 * условиях, т.к. цена должна рухнуть до 0.50, чтобы это случилось.
 *
 * ⚠️ ВАЖНО, КАК И РАНЬШЕ: разбор ответа getOrder() (parseGtcOrderStatus)
 * сделан по типичной схеме Polymarket CLOB (status: LIVE/MATCHED/CANCELED,
 * size_matched: число) БЕЗ реального примера ответа под рукой. Код
 * логирует СЫРОЙ ответ на каждой проверке — если разбор окажется
 * неточным, это будет видно в логах [tp-poll]/[tp-check].
 *
 * DRY_RUN=true по умолчанию (FASTFLIP_DRY_RUN=false для реальных денег).
 * ВАЖНО: баг №1/№2 выше воспроизводится ТОЛЬКО в LIVE — в DRY_RUN нет
 * реального стакана и реального проскальзывания, поэтому тестировать
 * этот фикс можно только в LIVE, на минимальном размере сделки.
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

// v5.6: ФИКС. Раньше было 0.002 — слишком жёстко для рыночного входа в
// последние секунды перед закрытием окна, когда стакан тонкий и цена
// естественно "проезжает" пару тиков при исполнении. Именно узкий
// допуск здесь в сочетании с багом №1 (см. шапку файла) вызывал
// мгновенные аварийные продажи сразу после нормальной на вид покупки.
// Теперь допуск шире — но всё ещё достаточно узкий, чтобы ловить
// реально плохие филлы, а не нормальное проскальзывание тонкого рынка.
const MAX_ENTRY_OVERSHOOT_TOLERANCE = Number(process.env.FASTFLIP_MAX_ENTRY_OVERSHOOT_TOLERANCE ?? "0.008");

// После закрытия окна ждём чуть-чуть (на случай гонки с последним тиком
// цены), и если ни тейк, ни стоп так и не сработали — идём резолвить
// через Gamma API.
const CLOSE_FALLBACK_BUFFER_MS = 5 * 1000;

const RESOLVE_CHECK_DELAY_SEC = 180;
const RESOLVE_RETRY_MS = 30 * 1000;
const RESOLVE_GIVE_UP_MS = 30 * 60 * 1000;

// v5.5: как часто перепроверяем статус висящего лимитника на выход, и
// с какой паузой повторяем ПОПЫТКУ ЕГО ВЫСТАВИТЬ, если предыдущая
// попытка не удалась (сетевая ошибка/отказ биржи и т.п.).
const TP_POLL_INTERVAL_MS = Number(process.env.FASTFLIP_TP_POLL_INTERVAL_MS ?? "2000");
const TP_PLACEMENT_RETRY_MS = Number(process.env.FASTFLIP_TP_PLACEMENT_RETRY_MS ?? "300");

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
  tpPrice: Number(process.env.FASTFLIP_TP_PRICE ?? "0.99"),
  // Стоп-лосс: АБСОЛЮТНАЯ цена токена (0.0–1.0), а не доля от цены
  // входа. Если цена после входа падает ДО этого уровня ИЛИ НИЖЕ —
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
  // v5.5: цена, по которой выставлен (или ещё выставляется) висящий
  // лимитник на выход в плюс — это max(tpPrice, buyPrice+маржа).
  tpLimitPrice: number;
  // v5.5: id висящего лимитника на бирже, если он уже успешно
  // выставлен. null, пока идут попытки выставить, или если он уже
  // снят/исполнен и обнулён после сверки.
  tpOrderId: string | null;
  // v5.5: таймер периодической проверки статуса висящего лимитника.
  tpPollTimer: NodeJS.Timeout | null;
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

/**
 * Самопроверяющийся разбор ответа биржи на предмет "что тут акции, а
 * что доллары" — используется ТОЛЬКО для рыночных (taker) ордеров
 * (вход и стоп-лосс), где мы не выбираем цену исполнения сами. Для
 * висящего лимитника на выход это не нужно — см. parseGtcOrderStatus.
 *
 * Пробуем ОБА варианта раскладки чисел rawA/rawB, считаем получившуюся
 * среднюю цену в каждом варианте, и оставляем тот, где цена получается
 * РЕАЛЬНОЙ (строго между 0 и 1) и БЛИЖЕ к ожидаемой цене сделки.
 *
 * v5.6: expectedPrice для входа теперь передаётся как реальная цена
 * тика на момент входа (priceAtEntry), а не фиксированный
 * settings.entryPrice — см. фикс в executeMarketEntry.
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

/**
 * Разбор ответа getOrder() для НАШЕГО СОБСТВЕННОГО висящего лимитника.
 * В отличие от resolveFill() выше, тут не нужно гадать цену — она у
 * нас уже известна (это та цена, что мы сами поставили). Нужно только
 * понять: жив ли ордер ещё, исполнен ли (полностью/частично), или
 * отменён.
 *
 * ⚠️ Поля ниже (status, size_matched) — по типичной схеме Polymarket
 * CLOB (status: LIVE/MATCHED/CANCELED). Точного примера ответа под
 * рукой не было на момент написания — функция логирует сырой ответ
 * при каждом вызове (см. вызовы ниже), чтобы расхождение в названиях
 * полей сразу было видно в логах и его можно было поправить.
 */
function parseGtcOrderStatus(raw: unknown): {
  status: "LIVE" | "MATCHED" | "CANCELED" | "UNKNOWN";
  sizeMatched: number;
} {
  const obj = (raw ?? {}) as Record<string, unknown>;

  const rawStatus = String(obj.status ?? obj.state ?? "").toUpperCase();
  let status: "LIVE" | "MATCHED" | "CANCELED" | "UNKNOWN" = "UNKNOWN";
  if (rawStatus.includes("MATCH") || rawStatus.includes("FILL")) status = "MATCHED";
  else if (rawStatus.includes("CANCEL")) status = "CANCELED";
  else if (rawStatus.includes("LIVE") || rawStatus.includes("OPEN")) status = "LIVE";

  const sizeMatchedRaw = obj.size_matched ?? obj.sizeMatched ?? obj.matchedAmount ?? obj.filledSize ?? 0;
  const sizeMatched = Number(sizeMatchedRaw) || 0;

  return { status, sizeMatched };
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
      `Режим: рыночный вход, лимитный выход (GTC), BTC\n` +
      `Вход: ${settings.entryPrice}–${settings.maxEntryPrice} | Тейк: ${settings.tpPrice} | Стоп: ${settings.slPrice} | Окно входа: последние ${settings.entryWindowSec}с\n` +
      `Допуск на овершут входа: ${MAX_ENTRY_OVERSHOOT_TOLERANCE}\n` +
      `Квота: ${this.tradesThisHour}/${settings.quotaPerHour} сделок в этом часе\n` +
      `Сейчас отслеживается активных 5-мин рынков: ${watchedMarkets}\n` +
      `Всего сделок с запуска: ${this.tradesTotal}\n` +
      `Открытая позиция: ${
        this.openPosition
          ? `${this.openPosition.market.title} (${this.openPosition.side}), цена входа ${this.openPosition.buyPrice.toFixed(4)}, лимитник на выход: ${this.openPosition.tpOrderId ? `выставлен по ${this.openPosition.tpLimitPrice.toFixed(3)}` : "ещё выставляется..."}`
          : "нет"
      }`
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
    // отчёте) и проверяем не пора ли выходить по стопу (и, в DRY_RUN,
    // по тейку — см. maybeTriggerExit) ──
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
    // покупать, пропускаем этот сетап (не гонимся за ценой). Это
    // проверка по цене ТИКА — окончательная проверка по РЕАЛЬНОЙ цене
    // исполнения делается после покупки, см. executeMarketEntry.
    if (price < settings.entryPrice) return;
    if (price > settings.maxEntryPrice) return;

    this.attemptInProgress = true;
    const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
    this.executeMarketEntry(market, side, tokenId, price, secToClose);
  }

  /**
   * Вызывается на каждый тик цены, пока позиция открыта. Порядок
   * проверок важен: СТОП проверяется ПЕРВЫМ, до тейка.
   *
   * Тейк в LIVE-режиме больше НЕ проверяется здесь — он исполняется
   * сам как висящий лимитник на бирже (см. placeTpLimitWithRetry). Тик-
   * триггер на тейк остаётся только для DRY_RUN (там нет реального
   * стакана и реального лимитника, поэтому старая симуляция "по тику").
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
    // Работает одинаково в DRY_RUN и в LIVE. ВАЖНО: settings.slPrice —
    // это АБСОЛЮТНАЯ цена токена (0.0-1.0), а не доля от цены входа.
    if (settings.slPrice > 0 && price <= settings.slPrice) {
      pos.exitAttemptInProgress = true;
      this.executeStopLoss(pos, price);
      return;
    }

    // ── ТЕЙК по тику — ТОЛЬКО для DRY_RUN / когда нет реального клоба.
    // В LIVE-режиме тейк исполняется сам как висящий лимитник на бирже
    // (placeTpLimitWithRetry уже выставлен сразу после входа), поэтому
    // тут для LIVE ничего не делаем — дальше просто ждём: либо биржа
    // сама исполнит лимитник, либо сработает стоп выше, либо дойдём до
    // закрытия окна и уйдём на резолв (со сверкой, см. entry-таймер).
    if (!DRY_RUN && this.clob) return;

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

      // Самопроверяющийся разбор — см. комментарий у функции resolveFill.
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

  /**
   * Выставляет висящий лимитник (GTC) на продажу по цене
   * pos.tpLimitPrice. Если попытка ВЫСТАВИТЬ (не исполнить!) не удалась
   * — сразу пробует снова, с небольшой паузой, пока не получится или
   * пока позиция не закрылась другим способом (например, сработал стоп
   * раньше, чем мы успели выставить тейк). Как только ордер успешно
   * принят биржей — запускает периодическую проверку его статуса.
   *
   * В DRY_RUN / без реального клоба — ничего не делает: там тейк по-
   * прежнему симулируется по тику цены (см. maybeTriggerExit).
   */
  private async placeTpLimitWithRetry(pos: OpenPosition): Promise<void> {
    if (DRY_RUN || !this.clob) return;

    let attempt = 0;
    while (!pos.closed && pos.tpOrderId === null) {
      attempt++;
      try {
        const resp = await this.clob.placeGtcLimitOrder({
          tokenId: pos.tokenId,
          side: Side.SELL,
          price: pos.tpLimitPrice,
          size: pos.filledSize,
          // offsetPct: 0 — хотим повесить ордер РОВНО на tpLimitPrice,
          // без дополнительного агрессивного сдвига (тот сдвиг нужен
          // для других сценариев использования этого метода, не для
          // нашего "поставил и жду").
          offsetPct: 0,
        });

        if (pos.closed) {
          // Пока ждали ответ биржи, позицию уже закрыли другим путём
          // (например, стоп успел сработать раньше). Пытаемся сразу
          // отменить только что выставленный ордер, чтобы не остался
          // висеть без дела.
          if (resp.orderId) {
            this.clob.cancelOrders([resp.orderId]).catch(() => {});
          }
          return;
        }

        pos.tpOrderId = resp.orderId ?? null;

        if (!pos.tpOrderId) {
          console.log(
            `   ⚠️ Лимитка на выход принята биржей, но orderId не вернулся — отследить статус не смогу, сверю при закрытии окна.`,
          );
          return;
        }

        console.log(
          `   📌 Лимитка на выход выставлена: цена ${pos.tpLimitPrice.toFixed(3)}, размер ${pos.filledSize.toFixed(2)}, orderId ${pos.tpOrderId} (попытка ${attempt}).`,
        );
        this.startTpPolling(pos);
        return;
      } catch (err) {
        console.log(
          `   ⏳ Не удалось выставить лимитку на выход (попытка ${attempt}): ${(err as Error).message}. Пробую снова через ${TP_PLACEMENT_RETRY_MS}мс.`,
        );
        await new Promise((r) => setTimeout(r, TP_PLACEMENT_RETRY_MS));
      }
    }
  }

  /** Периодически проверяет, не исполнился ли висящий лимитник на выход. */
  private startTpPolling(pos: OpenPosition): void {
    if (!this.clob) return;

    pos.tpPollTimer = setInterval(async () => {
      if (pos.closed || !pos.tpOrderId) {
        if (pos.tpPollTimer) {
          clearInterval(pos.tpPollTimer);
          pos.tpPollTimer = null;
        }
        return;
      }

      try {
        const raw = await this.clob!.getOrder(pos.tpOrderId);
        console.log(`   [tp-poll] статус лимитника ${pos.tpOrderId}:`, raw);
        const { status, sizeMatched } = parseGtcOrderStatus(raw);

        if (status === "MATCHED" && sizeMatched >= pos.filledSize - 0.001) {
          if (pos.tpPollTimer) {
            clearInterval(pos.tpPollTimer);
            pos.tpPollTimer = null;
          }
          if (pos.resolveFallbackTimer) {
            clearTimeout(pos.resolveFallbackTimer);
            pos.resolveFallbackTimer = null;
          }
          pos.closed = true;
          const profit = pos.filledSize * (pos.tpLimitPrice - pos.buyPrice);
          await this.notifyClose(pos, "тейк лимиткой", "WIN", profit);
          this.finishTrade(pos);
        } else if (status === "CANCELED") {
          console.log(`   ⚠️ Лимитник на выход оказался отменён (не нами) — выставляю заново.`);
          if (pos.tpPollTimer) {
            clearInterval(pos.tpPollTimer);
            pos.tpPollTimer = null;
          }
          pos.tpOrderId = null;
          if (!pos.closed) this.placeTpLimitWithRetry(pos);
        }
        // status === "LIVE" или частичное исполнение — просто ждём дальше.
      } catch (err) {
        console.error(`   [tp-poll] ошибка проверки статуса лимитника:`, (err as Error).message);
      }
    }, TP_POLL_INTERVAL_MS);
  }

  /**
   * Сверка перед тем, как забрать позицию у висящего лимитника (перед
   * стопом или перед уходом на резолв). Останавливает поллинг,
   * проверяет актуальный статус, и если выясняется, что лимитник УЖЕ
   * полностью исполнился — сообщает об этом вызывающему коду, чтобы
   * тот закрыл сделку как "тейк лимиткой", а не продолжал считать
   * позицию открытой. Если не исполнился — пробует его отменить
   * (best-effort, ошибки игнорируем: возможно, он уже пропал сам по
   * другой причине) и возвращает "clear".
   */
  private async finalizeTpBeforeOverride(pos: OpenPosition): Promise<"already-filled" | "clear"> {
    if (pos.tpPollTimer) {
      clearInterval(pos.tpPollTimer);
      pos.tpPollTimer = null;
    }

    if (!pos.tpOrderId || !this.clob) return "clear";

    const orderId = pos.tpOrderId;

    try {
      const raw = await this.clob.getOrder(orderId);
      console.log(`   [tp-check] финальная сверка статуса лимитника ${orderId}:`, raw);
      const { status, sizeMatched } = parseGtcOrderStatus(raw);
      if (status === "MATCHED" && sizeMatched >= pos.filledSize - 0.001) {
        pos.tpOrderId = null;
        return "already-filled";
      }
    } catch (err) {
      console.error(`   [tp-check] не удалось проверить статус перед отменой:`, (err as Error).message);
    }

    try {
      await this.clob.cancelOrders([orderId]);
    } catch (err) {
      console.error(`   [tp-check] не удалось отменить лимитник (возможно, уже неактуален):`, (err as Error).message);
    }

    pos.tpOrderId = null;
    return "clear";
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

    // v5.6 ФИКС: раньше здесь передавался фиксированный settings.entryPrice
    // (0.97) как база для расчёта проскальзывания FAK-ордера, хотя
    // реальная цена тика (priceAtEntry) уже была известна на этот момент.
    // Из-за этого допустимый диапазон исполнения считался от неправильной
    // точки — теперь база = реальная цена тика.
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
      tpLimitPrice: Math.max(settings.tpPrice, result.avgPrice + MIN_PROFIT_MARGIN),
      tpOrderId: null,
      tpPollTimer: null,
    };
    this.openPosition = pos;

    // ── Проверка на аномальное исполнение СНИЗУ: купили НАМНОГО ниже,
    // чем ожидали — сразу пробуем аварийно выйти, не дожидаясь резолва.
    const isBadFillLow = pos.buyPrice < settings.entryPrice - BAD_FILL_TOLERANCE;

    // ── Проверка на аномальное исполнение СВЕРХУ — купили дороже
    // потолка коридора (settings.maxEntryPrice). Требование "никогда не
    // покупать дороже 0.98" — обеспечивается именно этой проверкой уже
    // ПОСЛЕ реального исполнения, а не только по цене тика до отправки.
    // v5.6: допуск расширен (MAX_ENTRY_OVERSHOOT_TOLERANCE = 0.008), см.
    // комментарий у константы выше.
    const isBadFillHigh = pos.buyPrice > settings.maxEntryPrice + MAX_ENTRY_OVERSHOOT_TOLERANCE;

    if (isBadFillLow || isBadFillHigh) {
      // v5.6: печатаем точную непокруглённую цену и разницу с порогом,
      // чтобы не гадать по округлённым 97/98 из интерфейса Polymarket.
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

    // Сразу, максимально быстро (не дожидаясь ничего другого), запускаем
    // выставление висящего лимитника на выход. Не await — это отдельный
    // самоподдерживающийся процесс (ретраи постановки + последующий
    // поллинг), не должен блокировать остальной код.
    this.placeTpLimitWithRetry(pos).catch((err) =>
      console.error(`   [tp] неожиданная ошибка в цикле выставления лимитника:`, (err as Error).message),
    );

    if (this.telegram) {
      const tpNote = DRY_RUN || !this.clob
        ? `\nЖду тейк ${settings.tpPrice} (симуляция DRY_RUN) или стоп ${settings.slPrice}, либо закрытия окна...`
        : `\nВыставляю лимитник на выход по ${pos.tpLimitPrice.toFixed(3)}, либо жду стоп ${settings.slPrice}, либо закрытие окна...`;

      await this.telegram.send(
        `💰 Куплено по рынку: ${market.title}\nСторона: ${side}\nЦена: ${result.avgPrice.toFixed(4)} | Размер: ${result.filledSize.toFixed(2)}${tpNote}`,
      );
    }

    const msUntilCloseCheck = Math.max(0, market.closeTimeMs - Date.now() + CLOSE_FALLBACK_BUFFER_MS);
    pos.resolveFallbackTimer = setTimeout(async () => {
      if (pos.closed) return;

      // Перед уходом на резолв — сверяемся, не исполнился ли лимитник в
      // последний момент, пока мы не смотрели (гонка "тик от резолва
      // против последнего тика цены").
      const state = await this.finalizeTpBeforeOverride(pos);
      if (pos.closed) return; // сверка могла сама закрыть сделку

      if (state === "already-filled") {
        const profit = pos.filledSize * (pos.tpLimitPrice - pos.buyPrice);
        pos.closed = true;
        await this.notifyClose(pos, "тейк лимиткой (обнаружено при сверке)", "WIN", profit);
        this.finishTrade(pos);
        return;
      }

      console.log(`   ⏳ Ни тейк, ни стоп не сработали до закрытия — жду официальный резолв (eventSlug: ${market.eventSlug}).`);
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

  /**
   * Старый метод "выйти рынком по тейку" — используется ТОЛЬКО в
   * DRY_RUN (см. gate в maybeTriggerExit). В LIVE-режиме тейк
   * закрывается через висящий лимитник, а не отсюда.
   */
  private async executeMarketExit(pos: OpenPosition, priceAtExit: number): Promise<void> {
    console.log(
      `\n🎯 ВЫХОД ПО РЫНКУ (ТЕЙК, DRY_RUN): [BTC / 5мин] "${pos.market.title}"\n` +
        `   Сторона: ${pos.side} | Цена сейчас: ~${priceAtExit} | Продаём: ${pos.filledSize.toFixed(2)} акций (цель ~${settings.tpPrice})`,
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
    await this.notifyClose(pos, "тейк по рынку (DRY_RUN)", "WIN", profit);
    this.finishTrade(pos);
  }

  private async executeStopLoss(pos: OpenPosition, priceAtExit: number): Promise<void> {
    console.log(
      `\n🛑 СТОП-ЛОСС: [BTC / 5мин] "${pos.market.title}"\n` +
        `   Сторона: ${pos.side} | Цена сейчас: ~${priceAtExit} (порог стопа: ${settings.slPrice}) | ` +
        `Продаём: ${pos.filledSize.toFixed(2)} акций рыночным ордером НЕМЕДЛЕННО`,
    );

    // ПЕРЕД тем как продавать по стопу — сверяемся с висящим лимитником
    // на выход. Если он на самом деле УЖЕ исполнился (гонка: цена
    // мазнула по тейку и тут же обвалилась до стопа) — закрываем сделку
    // как тейк, а НЕ пытаемся продать уже не существующую позицию по
    // стопу поверх неё.
    if (!DRY_RUN && this.clob) {
      const state = await this.finalizeTpBeforeOverride(pos);
      if (pos.closed) return;
      if (state === "already-filled") {
        const profit = pos.filledSize * (pos.tpLimitPrice - pos.buyPrice);
        pos.closed = true;
        await this.notifyClose(pos, "тейк лимиткой (обнаружено перед стопом)", "WIN", profit);
        this.finishTrade(pos);
        return;
      }
    }

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
    if (pos.tpPollTimer) {
      clearInterval(pos.tpPollTimer);
      pos.tpPollTimer = null;
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
      `Тейк: ${settings.tpPrice} (лимитником) | Стоп: ${settings.slPrice} (рынком) | Квота: ${settings.quotaPerHour}/час | ` +
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