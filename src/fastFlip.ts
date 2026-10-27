/**
 * "Быстрый флип" v7.15 — РЫНОЧНЫЕ ордера на ВХОД, держим ДО ОФИЦИАЛЬНОГО
 * РЕЗОЛВА. Никаких лимиток на выход и никаких продаж по тику цены.
 *
 * v7.15 (относительно v7.14):
 *  1) УБРАН СПИСОК ACTIVE_PROFILE_ORDER. Все 10 профилей проверяются по порядку и считаются включёнными по умолчанию.
 *     Отдельной логики включения/выключения профилей больше нет; условия и параметры самих стратегий не менялись.
 *
 * v7.14 (относительно v7.13):
 *  1) ДОБАВЛЕН ПРОФИЛЬ 10 «Модель 0.90-0.93» — ячейка из LAB «Модели цены»: сила z от 2 до 3, за 120-180с до закрытия,
 *     цена токена 0.90-0.93, живой bid не ниже 0.90. Новые поля профиля: minSecToClose (нижняя граница по времени)
 *     и maxZ (верхняя граница по z). ЭТО ТЕСТ: ячейка найдена на старых данных, проверять её надо на свежих.
 *  2) ИСПРАВЛЕН РАЗМЕР: акции = ставка / нижняя граница коридора профиля (раньше / 0.97), чтобы ордер не выходил меньше
 *     ставки в долларах при цене входа ниже 0.97.
 *
 * v7.12 (относительно v7.11): НЕСКОЛЬКО ПОЗИЦИЙ ОДНОВРЕМЕННО, как в парсере LAB. Раньше бот держал одну позицию и пропускал
 *  остальные сигналы. Теперь он может входить по нескольким рынкам сразу (по разным монетам в одном окне и в следующих окнах),
 *  но: одна позиция на рынок, не больше «лимит окно» позиций в одном окне (по умолчанию 3) и не больше «лимит всего»
 *  открытых позиций (по умолчанию 6, включая ожидающие резолва). Причина лимитов: монеты ходят вместе, и один разворот
 *  рынка бьёт сразу по всем позициям окна. Команды: «лимит окно N», «лимит всего N». Минимум цены позиции теперь
 *  отслеживается только по её собственному токену.
 *
 * v7.11 (относительно v7.10): БОТ БОЛЬШЕ НЕ БЛОКИРУЕТСЯ НА ВРЕМЯ ОЖИДАНИЯ РЕЗОЛВА. Раньше позиция держала блокировку
 *  до официального результата (после закрытия окна ещё 4-7 минут), и бот в это время не входил и не обновлял рынки.
 *  Теперь блокировка снимается через несколько секунд после закрытия окна (исход уже решён, ждём только результат).
 *  Пока окно позиции открыто, по-прежнему одна позиция за раз. Старая позиция дожидается резолва в фоне и присылает
 *  своё уведомление как раньше. В «статус» добавлена строка «Ждут резолва».
 *
 * v7.10 (относительно v7.9): ФИЛЬТР BINANCE у профиля 7. Бот сам опрашивает Binance (раз в секунду) и входит
 *  по профилю 7, только если Binance от цены открытия окна прошёл 50-80% от движения по потоку цен бота
 *  (так же считает LAB: «лаб бинанс», корзина «50-80% от потока»). Нет данных Binance — вход пропускается.
 *  Профиль 7: размер по умолчанию $5 (меньше $5 биржа не берёт целиком). Команды: «профиль N бинанс A B» (проценты) /
 *  «профиль N бинанс выкл».
 *
 * v7.9 (относительно v7.8): ДОБАВЛЕН ПРОФИЛЬ 7 «Модель» (z от 2.0, последние 180с, цена токена 0.93-0.96,
 *  живой bid не ниже 0.93) — по статистике «Модели цены» из LAB. Вшиты новые настройки профилей 1-6
 *  (размер $7 у всех, у профиля 7 — $3) — размеры больше НЕ берутся из переменных окружения.
 *
 * v7.8 (относительно v7.7): ИСПРАВЛЕН РЕЗОЛВ. Раньше бот сам угадывал победителя по ценам
 *  токенов через 3 минуты после закрытия и мог ошибиться (писал ПРИБЫЛЬ, когда на деле был
 *  убыток). Теперь победитель берётся только после официального закрытия рынка, из поля
 *  winner в CLOB; запасной вариант — только совсем крайние цены (0.99 / 0.01).
 *  Больше НИЧЕГО в логике торговли не менялось.
 *
 * v7.7 (относительно v7.6): у профиля 6 «Запас» добавлена проверка ЖИВОГО bid перед
 *  покупкой (requireLiveBid): бот входит, только если живой bid в стакане не ниже нижней
 *  границы коридора (0.97) — ровно как в парсере LAB, по статистике которого этот профиль
 *  подобран. Остальные профили поведение не меняют.
 *
 * v7.6 (относительно v7.5):
 *  1) ДОБАВЛЕН ПРОФИЛЬ 6 «ЗАПАС» (mode="margin"): вместо порога движения считается
 *     запас z = движение монеты в сторону токена ÷ (типичное 5-мин движение ×
 *     √(осталось секунд / 300)). Вход, если z не меньше заданного для монеты, в окне
 *     перед закрытием, при цене токена в общем коридоре 0.97-0.98 (с потолком покупки
 *     maxBuyPrice, как у остальных профилей).
 *  2) НАСТРОЙКИ ПРОФИЛЕЙ 1-5 ВШИТЫ В КОД (подобраны по статистике парсера), поэтому
 *     перезагрузка бота больше не сбрасывает их к старым значениям. Telegram-команды
 *     по-прежнему меняют настройки на лету, но только до следующей перезагрузки.
 *     Окна во всех профилях не больше 180с: окно от ~270с блокирует обновление списка
 *     рынков (см. hasCriticalMarket).
 *
 * Профили (сигнал ЛЮБОГО профиля достаточен для входа):
 *   1 — fixed (фиксированный % движения)
 *   2 — adaptive (множитель × типичное движение)
 *   3 — L1 отскок (касание 0.995 → 0.985-0.993)
 *   4 — L4 без цены (только движение монеты + время)
 *   5 — L5 повторное касание (0.97-0.98 → откат ниже 0.90 → снова 0.97-0.98)
 *   6 — «Запас» (z вместо порога)
 *   7 — «Модель» (z от 2.0, окно 180с, цена 0.93-0.96) + Binance 50-80%
 *   8 — «Запас» + Binance строго против потока
 *   9 — «Модель» + Binance строго против потока  ← основной
 *  10 — «Модель 0.90-0.93» (z 2-3, 120-180с до закрытия)  ← тест
 *
 * Управление профилями через Telegram:
 *   профиль <N> окно <секунды>
 *   профиль <N> окно <монета> <секунды>
 *   профиль <N> движение <монета> <значение>   (fixed: доля 0.0015; adaptive: множитель 1.5; запас: z, например 2.0)
 *   профиль <N> движение <значение>            (всем монетам профиля N разом)
 *   профиль <N> объем <доллары>
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
import { VolatilityTracker } from "./volatilityTracker.js";

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

// Пороги движения ПРОФИЛЯ 1 (основного), подобраны по статистике парсера.
const PROFILE_1_DEFAULT_THRESHOLDS: Record<string, number> = {
  Bitcoin: 0.0015,
  Ethereum: 0.003,
  Solana: 0.002,
  XRP: 0.003,
  Dogecoin: 0.0015,
};

// ─── Профили входа ───
// Профиль — это самостоятельный набор (окно входа + пороги по монетам).
//
// mode:
//  "fixed"    — pctMoveThresholds хранит ФИКСИРОВАННЫЙ % движения монеты (как раньше)
//  "adaptive" — pctMoveThresholds хранит МНОЖИТЕЛЬ (0.5 = "0.5x"), порог считается
//               налету как multiplier × recentVol (типичное 5-мин движение монеты
//               за последний час, через volTracker). Пока volTracker не накопит час
//               истории по монете — adaptive-профиль для этой монеты просто не
//               даёт сигналов (не считает на неполных данных).
//  "margin"   — «Запас»: pctMoveThresholds хранит минимальный z. Запас
//               z = движение монеты в сторону токена ÷ (recentVol × √(осталось/300)).
//               Как и adaptive, ждёт часа истории волатильности по монете.
//
// priceMode:
//  "corridor"   — обычная проверка: цена токена внутри [priceLow, priceHigh] (как раньше).
//                 Если priceLow/priceHigh не заданы — берутся общие settings.entryPrice/maxEntryPrice.
//  "touch-drop" — механика L1 ("100→99 отскок"): сначала цена ДОЛЖНА коснуться touchAbove
//                 (например 0.995), и только ПОСЛЕ ЭТОГО, когда цена окажется в [priceLow, priceHigh]
//                 (например 0.985-0.993) — считается сигналом. До касания touchAbove — не входит,
//                 даже если цена уже в приемлемом коридоре.
//  "none"       — механика L4 ("без цены, только % + время"): цена токена вообще НЕ проверяется,
//                 вход решается только движением монеты и временем до закрытия. priceLow/priceHigh
//                 не используются вообще (можно не задавать) — но ВАЖНО: maxBuyPrice для защиты от
//                 переплаты (см. clob.ts) в этом случае берётся из settings.maxEntryPrice, т.к. у
//                 профиля своего разумного предела цены нет по определению этой механики.
//  "retouch"    — механика L5 ("повторное касание"): цена ДОЛЖНА первый раз зайти в [priceLow,
//                 priceHigh] (это касание игнорируется, только запоминается), затем ОТКАТИТЬ ниже
//                 retouchDropBelow, и только после этого повторный заход в [priceLow, priceHigh]
//                 считается сигналом.
// Для "touch-drop" и "retouch" состояние (было ли касание/откат) хранится по каждому
// рынку/стороне/профилю отдельно и сбрасывается, когда рынок уходит из наблюдения.
interface EntryProfile {
  name: string;
  entryWindowSec: Record<string, number>;
  mode: "fixed" | "adaptive" | "margin";
  priceMode: "corridor" | "touch-drop" | "none" | "retouch";
  priceLow?: number; // свой коридор для профиля; если не задан — общий settings.entryPrice
  priceHigh?: number; // свой коридор для профиля; если не задан — общий settings.maxEntryPrice
  touchAbove?: number; // только для priceMode="touch-drop"
  retouchDropBelow?: number; // только для priceMode="retouch"
  pctMoveThresholds: Record<string, number>;
  sizeUsd: number; // размер ставки в USD именно для сделок этого профиля
  requireLiveBid?: boolean; // true — перед покупкой проверить ЖИВОЙ bid: он не должен быть ниже priceLow (как в парсере LAB)
  binanceRatio?: { min: number; max: number }; // фильтр: Binance прошёл от открытия окна [min, max) от движения по потоку бота (0.5-0.8 = 50-80%)
  binanceAgainst?: boolean; // отдельный вариант: направленное движение Binance строго против движения основного потока (ratio < 0)
  minSecToClose?: number; // v7.14: не входить, если до закрытия меньше стольки секунд (нижняя граница окна; верхняя — entryWindowSec)
  maxZ?: number; // v7.14: только для mode="margin": не входить, если z не меньше этого значения (верхняя граница силы движения)
}

function makeUniformThresholds(value: number): Record<string, number> {
  const out: Record<string, number> = {};
  for (const c of COINS) out[c] = value;
  return out;
}

/** Показывает значение порога профиля в правильных единицах: % для fixed, xN для adaptive, z≥N для запаса. */
function formatParamValue(profile: EntryProfile, coin: string): string {
  const v = profile.pctMoveThresholds[coin] ?? 0;
  if (profile.mode === "adaptive") return `${v.toFixed(1)}x`;
  if (profile.mode === "margin") return `z≥${v.toFixed(1)}${profile.maxZ !== undefined ? `, z<${profile.maxZ.toFixed(1)}` : ""}`;
  return `${(v * 100).toFixed(2)}%`;
}

/** Текстовое описание ценового условия профиля — единое место для getStatus() и стартового лога. */
function formatCorridorLabel(p: EntryProfile): string {
  const low = p.priceLow ?? settings.entryPrice;
  const high = p.priceHigh ?? settings.maxEntryPrice;
  switch (p.priceMode) {
    case "touch-drop":
      return `касание выше ${p.touchAbove} → ${low}-${high}`;
    case "retouch":
      return `${low}-${high} → откат ниже ${p.retouchDropBelow} → повтор ${low}-${high}`;
    case "none":
      return "без проверки цены";
    default:
      return `${low}-${high}`;
  }
}

const volTracker = new VolatilityTracker(PRICE_FEEDS);

// ─── Binance: цена монет, опрос REST раз в секунду, история за 15 минут (без внешних библиотек) ───
const BINANCE_SYMBOLS: Record<string, string> = {
  Bitcoin: "BTCUSDT",
  Ethereum: "ETHUSDT",
  Solana: "SOLUSDT",
  XRP: "XRPUSDT",
  Dogecoin: "DOGEUSDT",
};
const BINANCE_HOSTS = ["https://data-api.binance.vision", "https://api.binance.com"];
const BINANCE_POLL_MS = 1000;
const BINANCE_KEEP_MS = 15 * 60 * 1000;
const BINANCE_MATCH_MS = 4000; // допуск при поиске цены на момент открытия окна
const BINANCE_FRESH_MS = 5000; // последняя цена не старше этого

class BinanceFeed {
  private hist: Record<string, { t: number; p: number }[]> = {};
  private hostIdx = 0;
  private failStreak = 0;
  private polling = false;
  okCount = 0;
  failCount = 0;

  start(): void {
    void this.poll();
    setInterval(() => void this.poll(), BINANCE_POLL_MS);
  }

  private async poll(): Promise<void> {
    if (this.polling) return;
    this.polling = true;
    const symbols = Object.values(BINANCE_SYMBOLS);
    const url = `${BINANCE_HOSTS[this.hostIdx]}/api/v3/ticker/price?symbols=${encodeURIComponent(JSON.stringify(symbols))}`;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 3000);
    try {
      const resp = await fetch(url, { signal: ctrl.signal });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const arr: any[] = await resp.json();
      const t = Date.now();
      for (const row of arr) {
        const coin = Object.keys(BINANCE_SYMBOLS).find((c) => BINANCE_SYMBOLS[c] === row.symbol);
        const p = Number(row.price);
        if (!coin || !Number.isFinite(p) || p <= 0) continue;
        const h = this.hist[coin] ?? (this.hist[coin] = []);
        h.push({ t, p });
        const cutoff = t - BINANCE_KEEP_MS;
        while (h.length > 0 && h[0].t < cutoff) h.shift();
      }
      this.okCount++;
      this.failStreak = 0;
    } catch {
      this.failCount++;
      this.failStreak++;
      if (this.failStreak >= 5) {
        this.hostIdx = (this.hostIdx + 1) % BINANCE_HOSTS.length; // пробуем другой адрес
        this.failStreak = 0;
      }
    } finally {
      clearTimeout(timer);
      this.polling = false;
    }
  }

  getLatest(coin: string): number | null {
    const h = this.hist[coin];
    if (!h || h.length === 0) return null;
    const last = h[h.length - 1];
    return Date.now() - last.t <= BINANCE_FRESH_MS ? last.p : null;
  }

  getPriceAt(coin: string, ts: number): number | null {
    const h = this.hist[coin];
    if (!h || h.length === 0) return null;
    let best: { t: number; p: number } | null = null;
    let bestD = Infinity;
    for (let i = h.length - 1; i >= 0; i--) {
      const d = Math.abs(h[i].t - ts);
      if (d < bestD) {
        best = h[i];
        bestD = d;
      }
      if (h[i].t < ts - BINANCE_MATCH_MS) break;
    }
    return best !== null && bestD <= BINANCE_MATCH_MS ? best.p : null;
  }
}
const binance = new BinanceFeed();

// ─── Настройки, которые можно менять на лету через Telegram ───
// Значения ниже ВШИТЫ в код (подобраны по статистике парсера) — перезагрузка бота
// возвращает именно их. Команды в Telegram меняют их только до следующей перезагрузки.
const settings = {
  entryPrice: Number(process.env.FASTFLIP_ENTRY_PRICE ?? "0.97"),
  maxEntryPrice: Number(process.env.FASTFLIP_MAX_ENTRY_PRICE ?? "0.98"),
  // Квота фактически снята — 999 в час не является реальным ограничением,
  // частоту сделок регулируют только профили.
  quotaPerHour: Math.max(1, Number(process.env.FASTFLIP_QUOTA_PER_HOUR ?? "999")),
  // Лимиты одновременных позиций (меняются командами «лимит окно N» / «лимит всего N»).
  maxPerWindow: 3, // не больше стольких позиций в одном 5-минутном окне: один разворот рынка бьёт по всем сразу
  maxOpenTotal: 6, // не больше стольких открытых позиций всего (идущие + покупаемые + ждущие резолва)
  profiles: [
    {
      name: "Профиль 1 (основной, fixed)",
      entryWindowSec: {
        Bitcoin: 180,
        Ethereum: 60,
        Solana: 90,
        XRP: 180,
        Dogecoin: 90,
      },
      mode: "fixed",
      priceMode: "corridor",
      pctMoveThresholds: { ...PROFILE_1_DEFAULT_THRESHOLDS },
      sizeUsd: 7,
    },
    {
      // Для каждой монеты взята комбинация множитель/окно, где винрейт был
      // честно 100% на самой большой выборке. Bitcoin — исключение: честной сотни
      // нет, взят строгий множитель 3.0x «на запас».
      name: "Профиль 2 (adaptive)",
      entryWindowSec: {
        Bitcoin: 120,
        Ethereum: 30,
        Solana: 30,
        XRP: 30,
        Dogecoin: 60,
      },
      mode: "adaptive",
      priceMode: "corridor",
      pctMoveThresholds: {
        Bitcoin: 3.0,
        Ethereum: 0.3,
        Solana: 0.5,
        XRP: 0.3,
        Dogecoin: 1.0,
      },
      sizeUsd: 7,
    },
    {
      // L1 из research-grid-levels: "100→99 отскок". Цена ДОЛЖНА сначала
      // коснуться touchAbove (~1.00), и только затем, когда осядет в
      // priceLow-priceHigh (~0.985-0.993), считается сигналом. Отдельная
      // механика от Профилей 1/2 — не просто коридор, а состояние с памятью
      // по каждому рынку. Выборки в research маленькие — размер ставки
      // намеренно небольшой.
      name: "Профиль 3 (L1 отскок)",
      entryWindowSec: {
        Bitcoin: 180,
        Ethereum: 180,
        Solana: 90,
        XRP: 180,
        Dogecoin: 90,
      },
      mode: "fixed",
      priceMode: "touch-drop",
      touchAbove: 0.995,
      priceLow: 0.985,
      priceHigh: 0.993,
      pctMoveThresholds: {
        Bitcoin: 0.0013,
        Ethereum: 0.0014,
        Solana: 0.0015,
        XRP: 0.0015,
        Dogecoin: 0.0015,
      },
      sizeUsd: 7,
    },
    {
      // L4 из research-grid-levels: "без цены, только % + время". Цена
      // токена вообще не проверяется — чистый тест движения монеты. Пороги
      // подобраны по самой большой доступной 100%-выборке на монету.
      name: "Профиль 4 (L4 без цены)",
      entryWindowSec: {
        Bitcoin: 10,
        Ethereum: 10,
        Solana: 10,
        XRP: 30,
        Dogecoin: 60,
      },
      mode: "fixed",
      priceMode: "none",
      pctMoveThresholds: {
        Bitcoin: 0.002, // 0.20%
        Ethereum: 0.001, // 0.10%
        Solana: 0.001, // 0.10%
        XRP: 0.0014, // 0.14%
        Dogecoin: 0.003, // 0.30%
      },
      sizeUsd: 7,
    },
    {
      // L5 из research-grid-levels: "повторное касание 97-98". Первое
      // касание коридора игнорируется, нужен реальный откат ниже 0.90,
      // вход только на повторном заходе в коридор. Событие редкое —
      // размер ставки минимальный.
      name: "Профиль 5 (L5 повторное касание)",
      entryWindowSec: {
        Bitcoin: 180,
        Ethereum: 180,
        Solana: 180,
        XRP: 180,
        Dogecoin: 120,
      },
      mode: "fixed",
      priceMode: "retouch",
      priceLow: 0.97,
      priceHigh: 0.98,
      retouchDropBelow: 0.90,
      pctMoveThresholds: {
        Bitcoin: 0.001,
        Ethereum: 0.0013,
        Solana: 0.002,
        XRP: 0.001,
        Dogecoin: 0.001,
      },
      sizeUsd: 7,
    },
    {
      // «Запас» из парсера LAB: вход, когда монета уже прошла в сторону токена
      // больше, чем обычно успевает за оставшееся время (z = движение ÷
      // (типичное 5-мин движение × √(осталось/300))). Цена токена — общий
      // коридор 0.97-0.98, потолок покупки как у остальных профилей.
      // Значения z и окон — лучшие по статистике парсера на каждую монету
      // (честные 100%). Новая стратегия — размер ставки намеренно небольшой.
      name: "Профиль 6 (Запас)",
      entryWindowSec: {
        Bitcoin: 60,
        Ethereum: 180,
        Solana: 60,
        XRP: 180,
        Dogecoin: 120,
      },
      mode: "margin",
      priceMode: "corridor",
      pctMoveThresholds: {
        Bitcoin: 2.0,
        Ethereum: 4.0,
        Solana: 1.5,
        XRP: 2.5,
        Dogecoin: 1.0,
      },
      sizeUsd: 7,
      requireLiveBid: true, // в парсере вход записывается только при живом bid в коридоре 0.97-0.98
    },
    {
      // «Модель» из LAB («Модель цены»): сила движения z от 2.0, последние 180с,
      // цена токена 0.93-0.96 (дешевле, чем у остальных профилей), живой bid не ниже 0.93.
      // Потолок покупки = 0.96 (priceHigh). Новая стратегия — ставка небольшая.
      name: "Профиль 7 (Модель)",
      entryWindowSec: {
        Bitcoin: 180,
        Ethereum: 180,
        Solana: 180,
        XRP: 180,
        Dogecoin: 180,
      },
      mode: "margin",
      priceMode: "corridor",
      priceLow: 0.92,
      priceHigh: 0.98,
      pctMoveThresholds: {
        Bitcoin: 2.0,
        Ethereum: 2.0,
        Solana: 2.0,
        XRP: 2.0,
        Dogecoin: 2.0,
      },
      sizeUsd: 5,
      requireLiveBid: true,
      binanceRatio: { min: 0.5, max: 0.8 }, // Binance прошёл 50-80% от движения по потоку бота
    },
  ] as EntryProfile[],
};

// Отдельные исследовательские варианты 8/9: те же настройки профилей 6/7,
// но Binance обязан двигаться строго против направленного движения основного потока.
// Оригинальные профили 6/7 и их фильтры оставлены без изменений.
const profile6 = settings.profiles[5];
const profile7 = settings.profiles[6];
settings.profiles.push(
  {
    ...profile6,
    name: "Профиль 8 (Запас — Binance против)",
    entryWindowSec: { ...profile6.entryWindowSec },
    pctMoveThresholds: { ...profile6.pctMoveThresholds },
    binanceRatio: undefined,
    binanceAgainst: true,
  },
  {
    ...profile7,
    name: "Профиль 9 (Модель — Binance против)",
    entryWindowSec: { ...profile7.entryWindowSec },
    pctMoveThresholds: { ...profile7.pctMoveThresholds },
    binanceRatio: undefined,
    binanceAgainst: true,
  },
);

// v7.14: Профиль 10 «Модель 0.90-0.93» — ячейка из LAB «Модель цены»:
// сила z от 2 до 3, за 120-180с до закрытия, цена токена 0.90-0.93, живой bid не ниже 0.90.
// ЭТО ТЕСТ: ячейка найдена на старых данных. Проверять по «лаб бот» (bot10) на свежих данных.
settings.profiles.push({
  name: "Профиль 10 (Модель 0.90-0.93)",
  entryWindowSec: {
    Bitcoin: 180,
    Ethereum: 180,
    Solana: 180,
    XRP: 180,
    Dogecoin: 180,
  },
  mode: "margin",
  priceMode: "corridor",
  priceLow: 0.9,
  priceHigh: 0.93,
  pctMoveThresholds: makeUniformThresholds(2.0),
  sizeUsd: 5,
  requireLiveBid: true,
  minSecToClose: 120,
  maxZ: 3,
});

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
  released?: boolean; // окно закрылось, блокировка снята, позиция ждёт официального резолва в фоне
}

function buildTokenIndex(markets: CryptoUpDownMarket[]): Map<string, TokenInfo> {
  const idx = new Map<string, TokenInfo>();
  for (const m of markets) {
    idx.set(m.upTokenId, { market: m, side: "Up" });
    idx.set(m.downTokenId, { market: m, side: "Down" });
  }
  return idx;
}

/**
 * Официальный резолв рынка. Это ЕДИНСТВЕННЫЙ способ закрыть сделку.
 * v7.8: победитель берётся только когда рынок официально закрыт, из поля winner в CLOB.
 * Запасной вариант — только совсем крайние цены (0.99 / 0.01).
 */
async function resolveWinner(eventSlug: string): Promise<"Up" | "Down" | null> {
  try {
    const resp = await fetch(`${GAMMA_HOST}/events/slug/${eventSlug}`);
    if (!resp.ok) return null;
    const event = await resp.json();
    const market = (event.markets ?? [])[0];
    if (!market) return null;

    // 1. Рынок должен быть официально закрыт и подтверждён
    if (market.closed !== true) return null;
    if (typeof market.umaResolutionStatus === "string" && market.umaResolutionStatus !== "resolved") return null;

    // 2. Главный источник — официальное поле winner в CLOB
    if (market.conditionId) {
      try {
        const r = await fetch(`https://clob.polymarket.com/markets/${market.conditionId}`);
        if (r.ok) {
          const m: any = await r.json();
          const win = (m.tokens ?? []).find((t: any) => t.winner === true);
          if (win) {
            const o = String(win.outcome).trim().toLowerCase();
            console.log(`[resolve] ${eventSlug}: CLOB winner = ${o}`);
            if (o === "up") return "Up";
            if (o === "down") return "Down";
          }
        }
      } catch {
        // упало — идём по запасному варианту ниже
      }
    }

    // 3. Запасной вариант: цены, но только совсем крайние (0.99 / 0.01)
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
    console.log(`[resolve] ${eventSlug}: по ценам Up=${upPrice} Down=${downPrice}`);
    if (upPrice >= 0.99 && downPrice <= 0.01) return "Up";
    if (downPrice >= 0.99 && upPrice <= 0.01) return "Down";
    return null;
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

  private attempting = new Set<string>(); // рынки (eventSlug), по которым прямо сейчас идёт покупка
  private enteredMarkets = new Map<string, number>(); // eventSlug -> closeTimeMs: рынки, где уже есть позиция или идёт покупка (одна позиция на рынок)
  private positions = new Set<OpenPosition>(); // позиции, у которых окно ещё идёт
  private awaiting = new Set<OpenPosition>(); // позиции, у которых окно уже закрылось и которые ждут официального резолва

  /** Сколько позиций (или покупок) уже есть в окне, закрывающемся в closeTimeMs. */
  private windowCount(closeTimeMs: number): number {
    let n = 0;
    for (const c of this.enteredMarkets.values()) if (c === closeTimeMs) n++;
    return n;
  }

  /** Сколько позиций открыто всего: идущие + покупаемые + ждущие резолва. */
  private openTotal(): number {
    return this.positions.size + this.attempting.size + this.awaiting.size;
  }

  private beginAttempt(market: CryptoUpDownMarket): void {
    this.attempting.add(market.eventSlug);
    this.enteredMarkets.set(market.eventSlug, market.closeTimeMs);
  }
  private updateCount = 0;
  private tradesTotal = 0;

  private currentHourKey: number | null = null;
  private tradesThisHour = 0;

  // зафиксированная цена монеты на момент открытия окна, по eventSlug
  private openPrices: Map<string, number> = new Map();

  // Состояние "памяти" для профилей с priceMode="touch-drop" (L1) или
  // "retouch" (L5). Ключ: `${profileIdx}:${eventSlug}:${side}`.
  //  touchedHigh      — для touch-drop: было ли касание touchAbove
  //  touchedCorridor  — для retouch: было ли первое (игнорируемое) касание коридора
  //  leftAfterTouch   — для retouch: откатывала ли цена ниже retouchDropBelow после первого касания
  private touchStates: Map<string, { touchedHigh: boolean; touchedCorridor: boolean; leftAfterTouch: boolean }> =
    new Map();

  constructor(
    private clob: ClobService | null,
    private telegram: ReturnType<typeof createTelegramNotifier>,
  ) {}

  private maxEntryWindowSec(): number {
    let max = 0;
    for (const p of settings.profiles) {
      for (const c of COINS) max = Math.max(max, p.entryWindowSec[c] ?? 0);
    }
    return max;
  }

  getStatus(): string {
    const watchedMarkets = this.tokenIndex.size / 2;
    const profilesLines = settings.profiles
      .map(
        (p, i) =>
          `  [${i + 1}] 🟢 В ТОРГОВЛЕ ${p.name} — размер $${p.sizeUsd}, режим ${p.mode}, коридор ${formatCorridorLabel(p)}${p.minSecToClose !== undefined ? `, не раньше чем за ${p.minSecToClose}с до закрытия` : ""}${p.binanceAgainst ? ", фильтр Binance: строго против потока (ratio < 0)" : p.binanceRatio ? `, фильтр Binance ${Math.round(p.binanceRatio.min * 100)}-${Math.round(p.binanceRatio.max * 100)}% от потока` : ""}, монета(окно/порог): ` +
          COINS.map((c) => `${c}(${p.entryWindowSec[c] ?? "?"}с/${formatParamValue(p, c)})`).join(", "),
      )
      .join("\n");
    const text =
      `Режим: рыночный вход, держим ДО РЕЗОЛВА (без выхода по лимитке), монеты: ${COINS.join(", ")}\n` +
      `Коридор по умолчанию (если у профиля свой не задан): ${settings.entryPrice}–${settings.maxEntryPrice}\n` +
      `Профили входа (все профили включены; сигнал ЛЮБОГО профиля достаточен для входа):\n${profilesLines}\n` +
      `Квота: ${this.tradesThisHour}/${settings.quotaPerHour} сделок в этом часе\n` +
      `Сейчас отслеживается активных 5-мин рынков: ${watchedMarkets}\n` +
      `Всего сделок с запуска: ${this.tradesTotal}\n` +
      `Открытые позиции (окно ещё идёт): ${
        this.positions.size > 0
          ? [...this.positions].map((p) => `${p.market.title} (${p.side}, ${p.buyPrice.toFixed(2)})`).join("; ")
          : "нет"
      }\n` +
      `Ждут резолва (окно закрыто): ${this.awaiting.size}\n` +
      `Лимиты: не больше ${settings.maxPerWindow} позиций в одном окне, не больше ${settings.maxOpenTotal} открытых всего (идёт покупка: ${this.attempting.size})`;
    return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
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

  private binBlockLogged = new Set<string>();

  /** Фильтр Binance профиля: Binance от открытия окна должен пройти [min, max) от движения по потоку бота. */
  private binanceCheck(
    profile: EntryProfile,
    profileIdx: number,
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    pctMove: number,
    openTimeMs: number,
  ): { ok: boolean; note?: string } {
    const rule = profile.binanceRatio;
    if (!rule && !profile.binanceAgainst) return { ok: true };
    const lo = rule ? Math.round(rule.min * 100) : null;
    const hi = rule ? Math.round(rule.max * 100) : null;
    const bo = binance.getPriceAt(market.coin, openTimeMs);
    const bn = binance.getLatest(market.coin);
    const pmDir = side === "Up" ? pctMove : -pctMove;
    let reason: string | null = null;
    let ratio = 0;
    if (bo === null || bn === null || bo === 0) {
      reason = "нет свежих данных Binance";
    } else if (pmDir <= 0) {
      reason = "движение по потоку не в нашу сторону";
    } else {
      const binDir = ((bn - bo) / bo) * (side === "Up" ? 1 : -1);
      ratio = binDir / pmDir;
      if (profile.binanceAgainst) {
        if (ratio >= 0) reason = `Binance не против потока: отношение ${(ratio * 100).toFixed(0)}%, нужно строго меньше 0%`;
      } else if (rule && (ratio < rule.min || ratio >= rule.max)) {
        reason = `Binance прошёл ${(ratio * 100).toFixed(0)}% от потока, нужно ${lo}-${hi}%`;
      }
    }
    if (reason !== null) {
      const key = `${market.eventSlug}:${profileIdx}`;
      if (!this.binBlockLogged.has(key)) {
        if (this.binBlockLogged.size > 500) this.binBlockLogged.clear();
        this.binBlockLogged.add(key);
        console.log(`   🚫 [${profile.name}] ${market.coin}: вход пропущен — ${reason}`);
      }
      return { ok: false };
    }
    return { ok: true, note: profile.binanceAgainst
      ? `Binance против потока: отношение ${(ratio * 100).toFixed(0)}% (строго < 0%)`
      : `Binance: прошёл ${(ratio * 100).toFixed(0)}% от потока (фильтр ${lo}-${hi}%)` };
  }

  async refreshMarkets(): Promise<void> {
    if (this.hasCriticalMarket()) return;

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
    for (const slug of this.enteredMarkets.keys()) {
      if (!activeSlugs.has(slug) && !this.attempting.has(slug)) this.enteredMarkets.delete(slug);
    }
    // чистим touch-state (профили priceMode="touch-drop") для тех же рынков —
    // ключ имеет вид `${profileIdx}:${eventSlug}:${side}`, eventSlug — средняя часть.
    for (const key of this.touchStates.keys()) {
      const eventSlug = key.split(":")[1];
      if (!activeSlugs.has(eventSlug)) this.touchStates.delete(key);
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

    // Обновляем "память" состояния для профилей с priceMode="touch-drop"
    // ИЛИ "retouch" ВСЕГДА, даже если сейчас открыта другая позиция —
    // иначе можем пропустить момент касания/отката, пока бот занят другой сделкой.
    for (let idx = 0; idx < settings.profiles.length; idx++) {
      const profile = settings.profiles[idx];
      if (profile.priceMode !== "touch-drop" && profile.priceMode !== "retouch") continue;

      const key = `${idx}:${market.eventSlug}:${side}`;
      let state = this.touchStates.get(key);
      if (!state) {
        state = { touchedHigh: false, touchedCorridor: false, leftAfterTouch: false };
        this.touchStates.set(key, state);
      }

      if (profile.priceMode === "touch-drop") {
        const touchAbove = profile.touchAbove ?? 0.995;
        if (price >= touchAbove) state.touchedHigh = true;
      } else {
        // retouch: запоминаем первое касание коридора, затем — реальный откат ниже порога
        const low = profile.priceLow ?? settings.entryPrice;
        const high = profile.priceHigh ?? settings.maxEntryPrice;
        const dropBelow = profile.retouchDropBelow ?? 0.90;
        if (price >= low && price <= high) state.touchedCorridor = true;
        if (state.touchedCorridor && price < dropBelow) state.leftAfterTouch = true;
      }
    }

    // Отслеживаем минимум цены собственного токена позиции (для статистики просадки в отчёте о резолве).
    // Закрытие сделки происходит ТОЛЬКО через официальный резолв рынка.
    for (const pos of this.positions) {
      if (pos.tokenId === update.tokenId && price < pos.minPriceSinceEntry) pos.minPriceSinceEntry = price;
    }

    // Одна позиция на рынок: если по этому рынку уже есть позиция или идёт покупка — новые сигналы по нему не нужны.
    if (this.enteredMarkets.has(market.eventSlug)) return;
    for (const pos of this.positions) if (pos.market.eventSlug === market.eventSlug) return;

    // Лимиты одновременных позиций (монеты ходят вместе — один разворот бьёт по всем позициям окна сразу).
    if (this.windowCount(market.closeTimeMs) >= settings.maxPerWindow) return;
    if (this.openTotal() >= settings.maxOpenTotal) return;

    this.checkHourlyReset();

    if (this.tradesThisHour >= settings.quotaPerHour) return; // квота часа выполнена — ждём новый час

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
    // (цена по своему priceMode + окно + порог движения), даёт сигнал на вход.
    // v7.15: проверяем все профили по порядку от №1 до последнего.
    for (let profileIdx = 0; profileIdx < settings.profiles.length; profileIdx++) {
      const profile = settings.profiles[profileIdx];
      if (!profile) continue;
      const windowForCoin = profile.entryWindowSec[market.coin];
      if (windowForCoin === undefined) continue; // монета без настроенного окна в этом профиле — пропускаем
      if (secToClose > windowForCoin) continue; // этот профиль ещё не "открылся" по времени для этой монеты
      if (profile.minSecToClose !== undefined && secToClose < profile.minSecToClose) continue; // уже слишком поздно для этого профиля

      const priceLow = profile.priceLow ?? settings.entryPrice;
      const priceHigh = profile.priceHigh ?? settings.maxEntryPrice;

      let priceOk: boolean;
      if (profile.priceMode === "none") {
        priceOk = true; // L4 — цена токена вообще не проверяется, только движение монеты и время
      } else if (profile.priceMode === "touch-drop") {
        const state = this.touchStates.get(`${profileIdx}:${market.eventSlug}:${side}`);
        priceOk = (state?.touchedHigh ?? false) && price >= priceLow && price <= priceHigh;
      } else if (profile.priceMode === "retouch") {
        const state = this.touchStates.get(`${profileIdx}:${market.eventSlug}:${side}`);
        // Входим только когда было И первое касание коридора, И реальный откат ниже
        // retouchDropBelow ПОСЛЕ него, И сейчас цена СНОВА в коридоре (повторное касание).
        priceOk =
          (state?.touchedCorridor ?? false) &&
          (state?.leftAfterTouch ?? false) &&
          price >= priceLow &&
          price <= priceHigh;
      } else {
        priceOk = price >= priceLow && price <= priceHigh; // "corridor" — обычная проверка
      }
      if (!priceOk) continue;

      const paramValue = profile.pctMoveThresholds[market.coin];
      if (paramValue === undefined) continue; // монета без настроенного порога в этом профиле — пропускаем

      // ── «Запас» (профиль mode="margin"): вместо порога считаем запас z ──
      if (profile.mode === "margin") {
        const recentVol = volTracker.getRecentVolatility(market.coin, Date.now());
        if (recentVol === null || recentVol <= 1e-6) continue; // ещё нет часа истории волатильности
        const dirMove = side === "Up" ? pctMove : -pctMove; // плюс = монета идёт в сторону токена
        if (dirMove <= 0) continue;
        const z = dirMove / (recentVol * Math.sqrt(Math.max(secToClose, 1) / 300));
        if (z < paramValue) continue; // paramValue тут — минимальный z
        if (profile.maxZ !== undefined && z >= profile.maxZ) continue; // v7.14: слишком сильное движение для этой ячейки

        const binM = this.binanceCheck(profile, profileIdx, market, side, pctMove, openTimeMs);
        if (!binM.ok) continue;

        this.beginAttempt(market);
        const tokenIdMargin = side === "Up" ? market.upTokenId : market.downTokenId;
        this.executeMarketEntry(market, side, tokenIdMargin, price, secToClose, pctMove, profile, z, binM.note);
        return; // сигнал найден — дальше профили не проверяем
      }

      let pctThreshold: number;
      if (profile.mode === "adaptive") {
        const recentVol = volTracker.getRecentVolatility(market.coin, Date.now());
        if (recentVol === null) continue; // ещё не накопили час истории волатильности по этой монете — пропускаем профиль
        pctThreshold = paramValue * recentVol; // paramValue тут — множитель (0.5 = "0.5x")
      } else {
        pctThreshold = paramValue; // fixed — paramValue уже готовый % (доля)
      }

      const passesMove = side === "Up" ? pctMove >= pctThreshold : pctMove <= -pctThreshold;
      if (!passesMove) continue;

      const binG = this.binanceCheck(profile, profileIdx, market, side, pctMove, openTimeMs);
      if (!binG.ok) continue;

      this.beginAttempt(market);
      const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
      this.executeMarketEntry(market, side, tokenId, price, secToClose, pctMove, profile, pctThreshold, binG.note);
      return; // сигнал найден — дальше профили не проверяем
    }
  }

  private async placeMarketOrder(params: {
    tokenId: string;
    side: "BUY" | "SELL";
    size: number;
    nominalPrice: number;
    maxBuyPrice?: number; // жёсткий потолок — не покупать дороже этой цены, см. clob.ts
    minBidPrice?: number; // не входить, если живой bid ниже этой цены, см. clob.ts
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
        maxBuyPrice: params.maxBuyPrice,
        minBidPrice: params.minBidPrice,
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
    pctThresholdUsed: number, // для fixed/adaptive — фактический порог (доля); для "margin" — фактический z
    binanceNote?: string, // пояснение про фильтр Binance (если у профиля он включён)
  ): Promise<void> {
    // v7.14: акции считаем от НИЖНЕЙ границы коридора профиля, чтобы сумма ордера не выходила меньше ставки в долларах.
    const size = profile.sizeUsd / (profile.priceLow ?? settings.entryPrice);
    const rawParam = profile.pctMoveThresholds[market.coin] ?? 0;
    const thresholdLabel =
      profile.mode === "adaptive"
        ? `${rawParam.toFixed(1)}x → фактический порог ${(pctThresholdUsed * 100).toFixed(3)}%`
        : profile.mode === "margin"
          ? `запас z≥${rawParam.toFixed(1)}${profile.maxZ !== undefined ? `, z<${profile.maxZ.toFixed(1)}` : ""} → фактический z ${pctThresholdUsed.toFixed(2)}`
          : `${(pctThresholdUsed * 100).toFixed(2)}%`;

    console.log(
      `\n⚡ ВХОД ПО РЫНКУ [${profile.name}]: [${market.coin} / 5мин] "${market.title}"\n` +
        `   Сторона: ${side} | Цена сейчас: ~${priceAtEntry} (коридор профиля ${profile.priceLow ?? settings.entryPrice}-${profile.priceHigh ?? settings.maxEntryPrice}${profile.priceMode === "touch-drop" ? `, touch≥${profile.touchAbove}` : ""}) | До закрытия: ${secToClose.toFixed(1)}с (окно для ${market.coin}: ${profile.minSecToClose !== undefined ? `${profile.minSecToClose}-` : ""}${profile.entryWindowSec[market.coin]}с)\n` +
        `   Движение ${market.coin} от открытия окна: ${(pctMove * 100).toFixed(3)}% (порог профиля: ${thresholdLabel})\n` +
        `   Покупаем: ${size.toFixed(2)} акций рыночным ордером (~$${profile.sizeUsd}) — держим до резолва` +
        (binanceNote ? `\n   ${binanceNote}` : ""),
    );

    const result = await this.placeMarketOrder({
      tokenId,
      side: "BUY",
      size,
      nominalPrice: priceAtEntry,
      maxBuyPrice: profile.priceHigh ?? settings.maxEntryPrice,
      minBidPrice: profile.requireLiveBid ? (profile.priceLow ?? settings.entryPrice) : undefined,
    });

    if (result.filledSize <= 0) {
      console.log(`   ⏳ Рыночная покупка не исполнилась (eventSlug: ${market.eventSlug}). Продолжаю мониторинг.`);
      this.attempting.delete(market.eventSlug);
      this.enteredMarkets.delete(market.eventSlug);
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
    this.attempting.delete(market.eventSlug);
    this.positions.add(pos);

    if (this.telegram) {
      await this.telegram.send(
        `💰 Куплено по рынку [${profile.name}]: ${market.title}\nСторона: ${side}\nЦена: ${result.avgPrice.toFixed(4)} | Размер: ${result.filledSize.toFixed(2)}\nДвижение ${market.coin} от открытия окна: ${(pctMove * 100).toFixed(3)}% (порог профиля: ${thresholdLabel})\nДержим до официального резолва (сделка ${this.tradesThisHour + 1}/${settings.quotaPerHour} в этом часе).${binanceNote ? `\n${binanceNote}` : ""}`,
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
      console.log(`   ⏳ Окно закрылось — жду официальный резолв в фоне, бот свободен для новых входов (eventSlug: ${market.eventSlug}).`);
      this.releaseLock(pos);
      this.scheduleResolveFallback(pos);
    }, msUntilCloseCheck);
  }

  /**
   * Окно позиции закрылось: исход уже решён, остаётся только дождаться официального результата.
   * Переводим позицию в «ждёт резолва» (она больше не считается идущей) и сразу обновляем список рынков.
   */
  private releaseLock(pos: OpenPosition): void {
    pos.released = true;
    this.positions.delete(pos);
    this.awaiting.add(pos);
    this.refreshMarkets().catch((err) => console.error("[refresh] ошибка:", (err as Error).message));
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
    this.awaiting.delete(pos);
    this.positions.delete(pos);
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
          `сделок в часе: ${this.tradesThisHour}/${settings.quotaPerHour}, всего: ${this.tradesTotal}, открыто: ${this.positions.size}, ждут резолва: ${this.awaiting.size}, Binance ок/ошибок: ${binance.okCount}/${binance.failCount} ---`,
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

        const limitWindowMatch = text.match(/^лимит\s+окно\s+(\d+)$/);
        if (limitWindowMatch) {
          settings.maxPerWindow = Math.max(1, Number(limitWindowMatch[1]));
          await telegram?.send(`Лимит позиций в одном окне: ${settings.maxPerWindow}.`);
          continue;
        }

        const limitTotalMatch = text.match(/^лимит\s+всего\s+(\d+)$/);
        if (limitTotalMatch) {
          settings.maxOpenTotal = Math.max(1, Number(limitTotalMatch[1]));
          await telegram?.send(`Лимит открытых позиций всего (включая ждущие резолва): ${settings.maxOpenTotal}.`);
          continue;
        }

        const quotaMatch = text.match(/^квота\s+(\d+)$/);
        if (quotaMatch) {
          settings.quotaPerHour = Math.max(1, Number(quotaMatch[1]));
          await telegram?.send(`Квота установлена: ${settings.quotaPerHour} сделок в час.`);
          continue;
        }

        // ─── Команды, адресованные КОНКРЕТНОМУ профилю: "профиль N ..." ───
        // "профиль N окно <монета> X" — окно для ОДНОЙ конкретной монеты профиля.
        const profileWindowCoinMatch = text.match(/^профиль\s+(\d+)\s+окно\s+(\S+)\s+(\d+)$/);
        if (profileWindowCoinMatch) {
          const idx = Number(profileWindowCoinMatch[1]) - 1;
          const profile = settings.profiles[idx];
          if (!profile) {
            await telegram?.send(`Нет профиля №${profileWindowCoinMatch[1]}. Всего профилей: ${settings.profiles.length}.`);
            continue;
          }
          const alias = profileWindowCoinMatch[2].toLowerCase();
          const coin = COIN_ALIASES[alias];
          if (!coin) {
            await telegram?.send(`Не узнал монету "${profileWindowCoinMatch[2]}". Варианты: btc, eth, sol, xrp, doge.`);
            continue;
          }
          profile.entryWindowSec[coin] = Number(profileWindowCoinMatch[3]);
          await telegram?.send(`[${profile.name}] окно входа для ${coin} установлено: последние ${profile.entryWindowSec[coin]}с перед закрытием`);
          continue;
        }

        // "профиль N окно X" — окно ВСЕМ монетам профиля разом.
        const profileWindowMatch = text.match(/^профиль\s+(\d+)\s+окно\s+(\d+)$/);
        if (profileWindowMatch) {
          const idx = Number(profileWindowMatch[1]) - 1;
          const profile = settings.profiles[idx];
          if (!profile) {
            await telegram?.send(`Нет профиля №${profileWindowMatch[1]}. Всего профилей: ${settings.profiles.length}.`);
            continue;
          }
          const value = Number(profileWindowMatch[2]);
          for (const c of COINS) profile.entryWindowSec[c] = value;
          await telegram?.send(`[${profile.name}] окно входа установлено для всех монет: последние ${value}с перед закрытием`);
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
            `[${profile.name}] порог движения для ${coin} установлен: ${formatParamValue(profile, coin)}`,
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
          await telegram?.send(
            `[${profile.name}] порог движения установлен для всех монет: ${formatParamValue(profile, COINS[0])}` +
              (profile.mode === "adaptive" ? " (множитель × recentVol)" : profile.mode === "margin" ? " (минимальный запас z)" : ""),
          );
          continue;
        }

        const profileSizeMatch = text.match(/^профиль\s+(\d+)\s+объем\s+([\d.]+)$/);
        if (profileSizeMatch) {
          const idx = Number(profileSizeMatch[1]) - 1;
          const profile = settings.profiles[idx];
          if (!profile) {
            await telegram?.send(`Нет профиля №${profileSizeMatch[1]}. Всего профилей: ${settings.profiles.length}.`);
            continue;
          }
          profile.sizeUsd = Number(profileSizeMatch[2]);
          await telegram?.send(`[${profile.name}] размер ставки установлен: $${profile.sizeUsd}`);
          continue;
        }

        // "профиль N бинанс A B" — фильтр Binance (проценты), "профиль N бинанс выкл" — отключить фильтр.
        const profileBinMatch = text.match(/^профиль\s+(\d+)\s+бинанс\s+(\d+(?:\.\d+)?)\s+(\d+(?:\.\d+)?)$/);
        if (profileBinMatch) {
          const idx = Number(profileBinMatch[1]) - 1;
          const profile = settings.profiles[idx];
          if (!profile) {
            await telegram?.send(`Нет профиля №${profileBinMatch[1]}. Всего профилей: ${settings.profiles.length}.`);
            continue;
          }
          const lo = Number(profileBinMatch[2]) / 100;
          const hi = Number(profileBinMatch[3]) / 100;
          if (!(hi > lo)) {
            await telegram?.send("Второе число должно быть больше первого, например: профиль 7 бинанс 50 80");
            continue;
          }
          profile.binanceRatio = { min: lo, max: hi };
          await telegram?.send(`[${profile.name}] фильтр Binance включён: вход только когда Binance прошёл ${profileBinMatch[2]}-${profileBinMatch[3]}% от движения по потоку бота`);
          continue;
        }
        const profileBinOffMatch = text.match(/^профиль\s+(\d+)\s+бинанс\s+выкл$/);
        if (profileBinOffMatch) {
          const idx = Number(profileBinOffMatch[1]) - 1;
          const profile = settings.profiles[idx];
          if (!profile) {
            await telegram?.send(`Нет профиля №${profileBinOffMatch[1]}. Всего профилей: ${settings.profiles.length}.`);
            continue;
          }
          profile.binanceRatio = undefined;
          await telegram?.send(`[${profile.name}] фильтр Binance выключен`);
          continue;
        }

        // ─── Старые команды без "профиль N" — обратная совместимость,
        // адресуются ПРОФИЛЮ 1 (основному) ───
        const windowMatch = text.match(/^окно\s+(\d+)$/);
        if (windowMatch) {
          const profile = settings.profiles[0];
          const value = Number(windowMatch[1]);
          for (const c of COINS) profile.entryWindowSec[c] = value;
          await telegram?.send(`[${profile.name}] окно входа установлено для всех монет: последние ${value}с перед закрытием`);
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
            `[${profile.name}] порог движения для ${coin} установлен: ${formatParamValue(profile, coin)}`,
          );
          continue;
        }

        const moveAllMatch = text.match(/^движение\s+([\d.]+)$/);
        if (moveAllMatch) {
          const profile = settings.profiles[0];
          const value = Number(moveAllMatch[1]);
          for (const c of COINS) profile.pctMoveThresholds[c] = value;
          await telegram?.send(`[${profile.name}] порог движения установлен для всех монет: ${formatParamValue(profile, COINS[0])}`);
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
  console.log("=== FASTFLIP BUILD: v7.15 — ВСЕ ПРОФИЛИ 1-10 ВКЛЮЧЕНЫ, РАЗМЕР ОТ НИЖНЕЙ ГРАНИЦЫ КОРИДОРА ===");
  console.log(`Режим: ${DRY_RUN ? "DRY_RUN (без реальных сделок, полная симуляция)" : "⚠️  LIVE — РЕАЛЬНЫЕ ДЕНЬГИ"}`);
  console.log(`Коридор по умолчанию (если у профиля свой не задан): ${settings.entryPrice}-${settings.maxEntryPrice} | Квота: ${settings.quotaPerHour}/час`);
  console.log(`В торговле профили: ${settings.profiles.map((_, i) => i + 1).join(", ")} (все включены)`);
  for (const [i, p] of settings.profiles.entries()) {
    console.log(
      `  Профиль ${i + 1} "${p.name}" [${p.mode}] ВКЛ, коридор ${formatCorridorLabel(p)}${p.minSecToClose !== undefined ? `, не раньше чем за ${p.minSecToClose}с до закрытия` : ""}${p.binanceAgainst ? ", Binance строго против потока (ratio < 0)" : p.binanceRatio ? `, Binance ${Math.round(p.binanceRatio.min * 100)}-${Math.round(p.binanceRatio.max * 100)}% от потока` : ""}, размер $${p.sizeUsd}, монета(окно/порог): ` +
        COINS.map((c) => `${c}(${p.entryWindowSec[c] ?? "?"}с/${formatParamValue(p, c)})`).join(", "),
    );
  }
  console.log(
    "Профили «Запас» (6, 8) и «Модель» (7, 9, 10) начнут давать сигналы по каждой монете только после ~1 часа сбора истории волатильности (volTracker).",
  );

  // запускаем все фиды цены (Polymarket RTDS, Chainlink TWAP) до старта самого бота
  btcPriceFeed.start();
  ethPriceFeed.start();
  solPriceFeed.start();
  xrpPriceFeed.start();
  dogePriceFeed.start();
  volTracker.start(); // нужен для adaptive-профилей (Профиль 2) и «Запаса» / «Модели» (Профили 6-10)
  binance.start(); // нужен для фильтра Binance (Профили 7-9)

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
        "профиль N окно X / профиль N окно <монета> X / профиль N объем X / профиль N движение <монета> X / профиль N движение X (всем монетам профиля) / " +
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
